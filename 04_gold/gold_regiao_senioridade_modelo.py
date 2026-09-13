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
        "tb_gold_regiao_senioridade_modelo": {
            "input_table": "workspace.tb_state_of_data_silver",
            "output_path": f"s3://{bucket_name}/data-output/gold/state-of-data/regiao_senioridade_modelo/",
            "metadata_key": "data-output/gold/_metadata/tb_gold_regiao_senioridade_modelo.json",
            "table_name": "tb_gold_regiao_senioridade_modelo",
            "database": "workspace",
            "partition_column": "ano_pesquisa",
            "dimensoes": ["regiao_onde_mora", "nivel_senioridade_agrupada", "modelo_trabalho_atual"],
            "coluna_salario": "faixa_salarial_ponto_medio",
            "anos_nao_perguntado": {"modelo_trabalho_atual": [2023]},
            "rotulo_nao_perguntado": "Não perguntado em {ano}",
            "ordenacao": ["regiao_onde_mora", "nivel_senioridade_agrupada", "modelo_trabalho_atual", "ano_pesquisa"],
            "builder": lambda spark, df, cfg, bucket: agregar_regiao_senioridade_modelo(df, cfg),
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

def agregar_regiao_senioridade_modelo(df, cfg: dict):
    logger = configure_logging()
    part = cfg["partition_column"]
    sal = cfg["coluna_salario"]
    dims = cfg["dimensoes"]
    anos_np = cfg["anos_nao_perguntado"]["modelo_trabalho_atual"]

    df_d = df.select(*dims, part, sal)
    # em 2023 não tinha a pergunta de modelo de trabalho: mantenho a quebra região x senioridade
    # e o modelo vira 'Não perguntado em 2023' (contagem ok, salário NULL)
    df_d = df_d.withColumn(
        "modelo_trabalho_atual",
        F.when(F.col(part).isin(anos_np), F.concat(F.lit(cfg["rotulo_nao_perguntado"].split("{")[0]), F.col(part).cast(StringType())))
         .otherwise(F.col("modelo_trabalho_atual"))
    )
    df_d = rotular_nulos(df_d, dims)

    df_agg = df_d.groupBy(*dims, part).agg(F.count("*").alias("contagem"), F.avg(sal).alias("salario_medio"))

    # célula especial -> salário NULL
    especial = F.col("modelo_trabalho_atual").startswith("Não perguntado")
    for d in dims:
        especial = especial | (F.col(d) == NAO_INFORMADO)
    df_agg = df_agg.withColumn("salario_medio", F.when(especial, F.lit(None)).otherwise(F.col("salario_medio")).cast(DoubleType()))
    df_agg = flag_amostra_baixa(df_agg, "contagem")

    df_out = df_agg.select(
        *[F.col(d).cast(StringType()).alias(d) for d in dims],
        F.col(part).cast(IntegerType()).alias(part),
        F.col("contagem").cast(LongType()).alias("contagem"),
        F.col("salario_medio"),
        F.col("amostra_baixa"),
    )
    drift = [d for dim in dims for d in detectar_drift(df_out, dim, part)]
    detalhes = {
        "drift": drift,
        "amostra_baixa_casos": casos_amostra_baixa(df_out, dims + [part], "contagem"),
        "referencias": {"respondentes_por_ano": {str(r[part]): r["qtd"] for r in df_out.groupBy(part).agg(F.sum("contagem").alias("qtd")).collect()}},
    }
    for d in drift:
        logger.warning("DRIFT %s", d)
    logger.info("Células com amostra baixa (<%s): %s", LIMITE_AMOSTRA_BAIXA, len(detalhes["amostra_baixa_casos"]))
    return df_out, detalhes


def build_metadata(table_cfg: dict, detalhes: dict, qtd_linhas: int) -> dict:
    return {
        "tabela": f"{table_cfg['database']}.{table_cfg['table_name']}",
        "location": table_cfg["output_path"],
        "origem": table_cfg["input_table"],
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "pergunta_respondida": "Como o modelo de trabalho (remoto/híbrido/presencial) se distribui por região e senioridade, e com que salário médio?",
        "particao": table_cfg["partition_column"],
        "qtd_linhas": qtd_linhas,
        "schema": [
            {"coluna": "regiao_onde_mora", "tipo": "string", "descricao": "região; NULL -> 'Não informado'"},
            {"coluna": "nivel_senioridade_agrupada", "tipo": "string", "descricao": "Júnior/Pleno/Sênior; NULL -> 'Não informado'"},
            {"coluna": "modelo_trabalho_atual", "tipo": "string", "descricao": "modelo; NULL -> 'Não informado'; 2023 -> 'Não perguntado em 2023'"},
            {"coluna": "ano_pesquisa", "tipo": "int", "descricao": "edição (partição)"},
            {"coluna": "contagem", "tipo": "bigint", "descricao": "respondentes na célula (todos, com ou sem salário)"},
            {"coluna": "salario_medio", "tipo": "double", "descricao": "média de faixa_salarial_ponto_medio (só não nulos); NULL em células especiais"},
            {"coluna": "amostra_baixa", "tipo": "boolean", "descricao": f"contagem < {LIMITE_AMOSTRA_BAIXA}"},
        ],
        "regras": {
            "nao_perguntado": "2023: modelo_trabalho_atual = 'Não perguntado em 2023' mantendo a quebra por região × senioridade (contagem preenchida, salário NULL)",
            "especiais": "'Não informado' em qualquer dimensão ou 'Não perguntado': salário NULL",
            "amostra_baixa": f"contagem < {LIMITE_AMOSTRA_BAIXA}",
        },
        "validacao_esperada": "SUM(contagem) por ano = 5293/5217/3495",
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
        logger.info("Registros lidos da Silver: %s", df_silver.count())

        df_gold, detalhes = table_cfg["builder"](spark, df_silver, table_cfg, bucket_name)
        df_gold = df_gold.orderBy(*table_cfg["ordenacao"])
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
