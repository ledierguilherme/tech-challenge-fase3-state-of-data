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
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType

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
        "tb_gold_barreiras_ia": {
            "input_table": "workspace.tb_state_of_data_silver",
            "output_path": f"s3://{bucket_name}/data-output/gold/state-of-data/barreiras_ia/",
            "metadata_key": "data-output/gold/_metadata/tb_gold_barreiras_ia.json",
            "table_name": "tb_gold_barreiras_ia",
            "database": "workspace",
            "partition_column": "ano_pesquisa",
            "prefixo_motivo": "ia_emp_motivo_",
            "anos": [2023, 2024, 2025],
            "ordenacao": ["ano_pesquisa", "motivo"],
            "builder": lambda spark, df, cfg, bucket: agregar_barreiras_ia(spark, df, cfg),
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




def totais_booleanos(spark, df, colunas: list, part: str, anos: list, total_expr=None):
    # total_expr permite trocar o denominador (aqui uso 'quem passou pela pergunta' em vez de não nulos da coluna)
    aggs = []
    for c in colunas:
        aggs.append(F.sum(F.col(c).cast("int")).alias(f"t__{c}"))
        aggs.append((F.count(F.col(c)) if total_expr is None else total_expr).alias(f"n__{c}"))
    linhas = []
    for r in df.filter(F.col(part).isin(anos)).groupBy(part).agg(*aggs).collect():
        for c in colunas:
            linhas.append({"coluna": c, "ano": r[part], "contagem_true": int(r[f"t__{c}"] or 0), "total_respondentes": int(r[f"n__{c}"] or 0)})
    return linhas


def montar_df_tidy(spark, linhas: list, campos: list):
    schema = StructType([StructField(n, t, True) for n, t in campos])
    return spark.createDataFrame([tuple(l[n] for n, _ in campos) for l in linhas], schema)


# ---- lógica da tabela

def agregar_barreiras_ia(spark, df, cfg: dict):
    logger = configure_logging()
    part = cfg["partition_column"]
    prefixo = cfg["prefixo_motivo"]
    cols = sorted(c for c in df.columns if c.startswith(prefixo))

    # denominador = quem passou pela pergunta (não nulo em pelo menos 1 dos 9 motivos), o mesmo pros 9
    passou = F.count(F.when(F.coalesce(*[F.col(c) for c in cols]).isNotNull(), 1))
    linhas = [
        {"motivo": l["coluna"][len(prefixo):], "fonte": l["coluna"], part: l["ano"],
         "contagem_true": l["contagem_true"], "total_respondentes": l["total_respondentes"]}
        for l in totais_booleanos(spark, df, cols, part, cfg["anos"], total_expr=passou)
    ]
    campos = [("motivo", StringType()), ("fonte", StringType()), (part, IntegerType()), ("contagem_true", LongType()), ("total_respondentes", LongType())]
    df_out = montar_df_tidy(spark, linhas, campos)
    df_out = df_out.withColumn("percentual", F.when(F.col("total_respondentes") > 0, F.col("contagem_true") / F.col("total_respondentes")).cast(DoubleType()))
    df_out = flag_amostra_baixa(df_out, "contagem_true")

    drift = detectar_drift(df_out, "motivo", part)
    detalhes = {
        "drift": drift,
        "amostra_baixa_casos": casos_amostra_baixa(df_out, ["motivo", part], "contagem_true"),
        "referencias": {l["fonte"] + "|" + str(l[part]): {"contagem_true": l["contagem_true"], "total_respondentes": l["total_respondentes"]}
                        for l in linhas if l["fonte"] == "ia_emp_motivo_casos_uso"},
    }
    for d in drift:
        logger.warning("DRIFT %s", d)
    return df_out, detalhes


def build_metadata(table_cfg: dict, detalhes: dict, qtd_linhas: int) -> dict:
    return {
        "tabela": f"{table_cfg['database']}.{table_cfg['table_name']}",
        "location": table_cfg["output_path"],
        "origem": table_cfg["input_table"],
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "pergunta_respondida": "Quais barreiras impedem as empresas de adotar IA generativa e como mudam por edição?",
        "particao": table_cfg["partition_column"],
        "qtd_linhas": qtd_linhas,
        "schema": [
            {"coluna": "motivo", "tipo": "string", "descricao": "sufixo de ia_emp_motivo_* (casos_uso, alucinacao, regulamentacao, seguranca_privacidade, roi, dados_nao_prontos, expertise, alta_direcao, propriedade_intelectual)"},
            {"coluna": "fonte", "tipo": "string", "descricao": "coluna da Silver"},
            {"coluna": "ano_pesquisa", "tipo": "int", "descricao": "edição (partição)"},
            {"coluna": "contagem_true", "tipo": "bigint", "descricao": "respondentes que marcaram o motivo"},
            {"coluna": "total_respondentes", "tipo": "bigint", "descricao": "quem passou pela pergunta (não nulo em qualquer um dos 9 motivos) no ano"},
            {"coluna": "percentual", "tipo": "double", "descricao": "contagem_true / total_respondentes"},
            {"coluna": "amostra_baixa", "tipo": "boolean", "descricao": f"contagem_true < {LIMITE_AMOSTRA_BAIXA}"},
        ],
        "regras": {"total": "igual para os 9 motivos do mesmo ano (quem viu a pergunta)"},
        "validacao_esperada": "ia_emp_motivo_casos_uso true 303/334/188; total_respondentes 823/974/587 (guarda-chuva não nula na Bronze)",
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
