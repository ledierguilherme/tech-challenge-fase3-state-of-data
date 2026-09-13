import json
import logging
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType, StringType

# categorias especiais: têm contagem, mas métricas NULL e ficam fora dos denominadores
NAO_INFORMADO = "Não informado"
OUTROS = "Outros"
TODAS = "Todas"   # agregado sem quebra por região
TODOS = "Todos"   # agregado sem quebra por senioridade
LIMITE_AMOSTRA_BAIXA = 30


def configure_logging() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger


def build_config(bucket_name: str) -> dict:
    return {
        "tb_gold_salario_por_perfil": {
            "input_table": "workspace.tb_state_of_data_silver",
            "output_path": f"s3://{bucket_name}/data-output/gold/state-of-data/salario_por_perfil/",
            "metadata_key": "data-output/gold/_metadata/tb_gold_salario_por_perfil.json",
            # reaproveita o top-N de cargo calculado no gold_perfil_mercado (lê do json dele, não recalcula)
            "top_cargos_metadata_key": "data-output/gold/_metadata/tb_gold_perfil_mercado.json",
            "table_name": "tb_gold_salario_por_perfil",
            "database": "workspace",
            "partition_column": "ano_pesquisa",
            "dimensoes": ["cargo_atual_agrupado", "nivel_senioridade_agrupada", "regiao_onde_mora"],
            "coluna_salario": "faixa_salarial_ponto_medio",
            # o cruzamento 3D ficou muito esparso (>80% das células com menos de 30 pessoas), então
            # gero também os agregados com região='Todas' e/ou senioridade='Todos' na mesma tabela
            "agregados": [
                {"regiao_onde_mora": TODAS},
                {"nivel_senioridade_agrupada": TODOS},
                {"regiao_onde_mora": TODAS, "nivel_senioridade_agrupada": TODOS},
            ],
        },
    }


# ---- helpers (mesmos nos outros scripts gold, copiei e colei)

def read_silver(spark, input_table: str):
    return spark.table(input_table)


def rotular_nulos(df, colunas: list):
    for c in colunas:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(NAO_INFORMADO)))
    return df


def flag_amostra_baixa(df, coluna_contagem: str = "contagem"):
    # < 30 respondentes na célula: não filtro, só marco (quem for usar decide)
    return df.withColumn("amostra_baixa", (F.col(coluna_contagem) < LIMITE_AMOSTRA_BAIXA).cast(BooleanType()))


def casos_amostra_baixa(df, colunas_chave: list, coluna_contagem: str = "contagem", limite: int = 10000):
    # lista das células com amostra baixa pro relatório consolidado
    rows = df.filter(F.col("amostra_baixa")).select(*colunas_chave, coluna_contagem).orderBy(coluna_contagem).limit(limite).collect()
    return [{**{c: r[c] for c in colunas_chave}, "contagem": r[coluna_contagem]} for r in rows]


def detectar_drift(df, coluna: str, part: str, especiais=(NAO_INFORMADO, OUTROS, TODAS, TODOS)):
    # rótulo que aparece num ano e some no outro = provável mudança de nome na pesquisa (foi assim que
    # apareceu o caso do Engenheiro/Arquiteto de Dados). Só olha os anos em que a coluna tem algum valor,
    # senão modelo_trabalho_atual acusaria drift em 2023 sendo que a pergunta não existia
    regulares = df.filter(~F.col(coluna).isin(list(especiais)) & ~F.col(coluna).startswith("Não perguntado"))
    anos_com_dados = sorted(r[part] for r in regulares.select(part).distinct().collect())
    presenca = {}
    for r in regulares.select(coluna, part).distinct().collect():
        presenca.setdefault(r[coluna], set()).add(r[part])
    drift = []
    for valor, anos in sorted(presenca.items()):
        ausentes = [a for a in anos_com_dados if a not in anos]
        if ausentes:
            drift.append({"coluna": coluna, "valor": valor, "anos_presentes": sorted(anos), "anos_ausentes": ausentes})
    return drift


def write_parquet_partitioned(spark, df, output_path: str, partition_column: str):
    # repartition pra ficar 1 arquivo por ano
    logger = configure_logging()

    df.repartition(partition_column).write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy(partition_column) \
        .parquet(output_path)

    logger.info("Dados particionados salvos por %s em %s", partition_column, output_path)


def register_table(spark, database: str, table_name: str, output_path: str):
    logger = configure_logging()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # drop + create pra o catálogo ficar limpo a cada execução
    spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name}
        USING parquet
        LOCATION '{output_path}'
        """
    )

    spark.sql(f"MSCK REPAIR TABLE {database}.{table_name}")
    logger.info("Tabela %s registrada com partições detectadas automaticamente", table_name)


def save_metadata(bucket_name: str, metadata_key: str, metadata: dict):
    logger = configure_logging()
    boto3.client("s3").put_object(
        Bucket=bucket_name,
        Key=metadata_key,
        Body=json.dumps(metadata, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Metadados salvos em: s3://%s/%s", bucket_name, metadata_key)


def load_json_s3(bucket_name: str, key: str) -> dict:
    body = boto3.client("s3").get_object(Bucket=bucket_name, Key=key)["Body"].read()
    return json.loads(body.decode("utf-8"))


# ---- lógica da tabela

def top_cargos_da_tabela_1(bucket_name: str, key: str) -> list:
    meta = load_json_s3(bucket_name, key)
    dim = next(d for d in meta["dimensoes"] if d["coluna"] == "cargo_atual_agrupado")
    if not dim.get("top_n_aplicado"):
        raise ValueError("tb_gold_perfil_mercado não aplicou top-N em cargo_atual_agrupado")
    return dim["top_n_valores"]


def agregar_salario_por_perfil(df, cfg: dict, top_cargos: list):
    logger = configure_logging()
    part = cfg["partition_column"]
    sal = cfg["coluna_salario"]
    dims = cfg["dimensoes"]

    # só quem informou salário - NULL não pode entrar como zero na média
    df_sal = df.filter(F.col(sal).isNotNull())

    df_sal = df_sal.withColumn(
        "cargo_atual_agrupado",
        F.when(F.col("cargo_atual_agrupado").isNull(), F.lit(NAO_INFORMADO))
         .when(F.col("cargo_atual_agrupado").isin(top_cargos), F.col("cargo_atual_agrupado"))
         .otherwise(F.lit(OUTROS))
    )
    df_sal = rotular_nulos(df_sal, ["nivel_senioridade_agrupada", "regiao_onde_mora"])

    def agregar(df_base):
        return df_base.groupBy(*dims, part).agg(
            F.count("*").alias("contagem"),
            F.avg(F.col(sal)).alias("salario_medio"),
            F.percentile_approx(F.col(sal), 0.5).alias("salario_mediana"),
        )

    # cruzamento completo + agregados (troco a dimensão pelo rótulo 'Todas'/'Todos' antes de agrupar)
    df_agg = agregar(df_sal)
    for agregado in cfg["agregados"]:
        df_a = df_sal
        for dim, rotulo in agregado.items():
            df_a = df_a.withColumn(dim, F.lit(rotulo))
        df_agg = df_agg.unionByName(agregar(df_a))

    # se alguma dimensão é 'Não informado' zero as métricas (mesma regra da tabela 1)
    especial = F.lit(False)
    for d in dims:
        especial = especial | (F.col(d) == NAO_INFORMADO)
    df_agg = (
        df_agg
        .withColumn("salario_medio", F.when(especial, F.lit(None)).otherwise(F.col("salario_medio")).cast(DoubleType()))
        .withColumn("salario_mediana", F.when(especial, F.lit(None)).otherwise(F.col("salario_mediana")).cast(DoubleType()))
    )
    df_agg = flag_amostra_baixa(df_agg, "contagem")

    df_out = df_agg.select(
        *[F.col(d).cast(StringType()).alias(d) for d in dims],
        F.col(part).cast(IntegerType()).alias(part),
        F.col("contagem").cast(LongType()).alias("contagem"),
        F.col("salario_medio"),
        F.col("salario_mediana"),
        F.col("amostra_baixa"),
    )

    df_regular = df_out.filter((F.col("regiao_onde_mora") != TODAS) & (F.col("nivel_senioridade_agrupada") != TODOS))
    detalhes = {
        "top_cargos_reaproveitados": top_cargos,
        "agregados": cfg["agregados"],
        "drift": [d for dim in dims for d in detectar_drift(df_regular, dim, part)],
        "qtd_linhas_cruzamento_completo": df_regular.count(),
        "qtd_linhas_agregados": df_out.count() - df_regular.count(),
        "amostra_baixa_casos": casos_amostra_baixa(df_out, dims + [part], "contagem"),
        "referencias": {
            "respondentes_com_salario_por_ano": {str(r[part]): r["qtd"] for r in df_sal.groupBy(part).agg(F.count("*").alias("qtd")).collect()},
        },
    }
    for d in detalhes["drift"]:
        logger.warning("DRIFT %s", d)
    logger.info("Células com amostra baixa (<%s): %s", LIMITE_AMOSTRA_BAIXA, len(detalhes["amostra_baixa_casos"]))
    return df_out, detalhes


def build_metadata(table_cfg: dict, detalhes: dict, qtd_linhas: int) -> dict:
    return {
        "tabela": f"{table_cfg['database']}.{table_cfg['table_name']}",
        "location": table_cfg["output_path"],
        "origem": table_cfg["input_table"],
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "pergunta_respondida": "Quanto ganha cada perfil (cargo × senioridade × região) e como isso evoluiu por edição?",
        "particao": table_cfg["partition_column"],
        "qtd_linhas": qtd_linhas,
        "schema": [
            {"coluna": "cargo_atual_agrupado", "tipo": "string", "descricao": "top-15 da tabela 1; demais -> 'Outros'; NULL -> 'Não informado'"},
            {"coluna": "nivel_senioridade_agrupada", "tipo": "string", "descricao": "Júnior/Pleno/Sênior; NULL -> 'Não informado'; 'Todos' = agregado sem quebra por senioridade"},
            {"coluna": "regiao_onde_mora", "tipo": "string", "descricao": "região; NULL -> 'Não informado'; 'Todas' = agregado sem quebra por região"},
            {"coluna": "ano_pesquisa", "tipo": "int", "descricao": "edição (partição)"},
            {"coluna": "contagem", "tipo": "bigint", "descricao": "respondentes com salário informado na célula"},
            {"coluna": "salario_medio", "tipo": "double", "descricao": "média de faixa_salarial_ponto_medio; NULL quando alguma dimensão é 'Não informado'"},
            {"coluna": "salario_mediana", "tipo": "double", "descricao": "percentile_approx(faixa_salarial_ponto_medio, 0.5); NULL idem"},
            {"coluna": "amostra_baixa", "tipo": "boolean", "descricao": f"contagem < {LIMITE_AMOSTRA_BAIXA}"},
        ],
        "regras": {
            "filtro": "faixa_salarial_ponto_medio IS NOT NULL antes de agregar (NULL não entra como zero)",
            "top_n": "cargo_atual_agrupado usa o top-15 calculado em tb_gold_perfil_mercado (metadados), não recalcula",
            "especiais": "células com 'Não informado' em qualquer dimensão: contagem preenchida, salário NULL",
            "agregados": "linhas adicionais com regiao_onde_mora='Todas' e/ou nivel_senioridade_agrupada='Todos' (células robustas); SUM(contagem) por ano deve ser feita só sobre o cruzamento completo (regiao<>'Todas' AND nivel<>'Todos')",
            "amostra_baixa": f"contagem < {LIMITE_AMOSTRA_BAIXA}",
        },
        "validacao_esperada": "SUM(contagem) por ano, só no cruzamento completo, = COUNT(faixa_salarial_ponto_medio) na Silver por ano",
        **detalhes,
    }


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])
    bucket_name = args["BUCKET_NAME"]
    logger = configure_logging()

    logger.info("Inicializando Glue Job...")
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    config = build_config(bucket_name)

    for table_key, table_cfg in config.items():
        logger.info("Processando tabela: %s", table_key)

        df_silver = read_silver(spark, table_cfg["input_table"])
        logger.info("silver: %s registros", df_silver.count())

        top_cargos = top_cargos_da_tabela_1(bucket_name, table_cfg["top_cargos_metadata_key"])
        logger.info("top cargos (da tabela 1): %s", top_cargos)

        df_gold, detalhes = agregar_salario_por_perfil(df_silver, table_cfg, top_cargos)
        df_gold = df_gold.orderBy(*table_cfg["dimensoes"], table_cfg["partition_column"])
        qtd_linhas = df_gold.count()
        logger.info("Linhas na Gold: %s", qtd_linhas)

        write_parquet_partitioned(spark, df_gold, table_cfg["output_path"], table_cfg["partition_column"])
        logger.info("Parquet gravado em: %s", table_cfg["output_path"])

        register_table(spark, table_cfg["database"], table_cfg["table_name"], table_cfg["output_path"])
        logger.info("Tabela registrada no Glue Catalog: %s", table_cfg["table_name"])

        save_metadata(bucket_name, table_cfg["metadata_key"], build_metadata(table_cfg, detalhes, qtd_linhas))

    logger.info("Job concluído com sucesso.")
    job.commit()


if __name__ == "__main__":
    main()
