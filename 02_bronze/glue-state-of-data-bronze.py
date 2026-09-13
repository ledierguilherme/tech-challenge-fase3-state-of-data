import json
import logging
import re
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit


def configure_logging() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger


def build_config(bucket_name: str) -> dict:
    return {
        "tb_pesquisa_2023_bronze": {
            "input_path": f"s3://{bucket_name}/data-input/bronze/pesquisa-2023/State_of_data_BR_2023_Kaggle - df_survey_2023.csv",
            "output_path": f"s3://{bucket_name}/data-output/bronze/pesquisa-2023/",
            "metadata_key": "data-output/bronze/_metadata/pesquisa-2023_colunas.json",
            "table_name": "tb_pesquisa_2023_bronze",
            "database": "workspace",
            "separator": ",",
            "ano_pesquisa": "2023",
        },
        "tb_pesquisa_2024_bronze": {
            "input_path": f"s3://{bucket_name}/data-input/bronze/pesquisa-2024/Final Dataset - State of Data 2024 - Kaggle - df_survey_2024.csv",
            "output_path": f"s3://{bucket_name}/data-output/bronze/pesquisa-2024/",
            "metadata_key": "data-output/bronze/_metadata/pesquisa-2024_colunas.json",
            "table_name": "tb_pesquisa_2024_bronze",
            "database": "workspace",
            "separator": ",",
            "ano_pesquisa": "2024",
        },
        "tb_pesquisa_2025_bronze": {
            "input_path": f"s3://{bucket_name}/data-input/bronze/pesquisa-2025/Final Dataset - State of Data 2025-2026 - Kaggle.csv",
            "output_path": f"s3://{bucket_name}/data-output/bronze/pesquisa-2025/",
            "metadata_key": "data-output/bronze/_metadata/pesquisa-2025_colunas.json",
            "table_name": "tb_pesquisa_2025_bronze",
            "database": "workspace",
            "separator": ",",
            "ano_pesquisa": "2025",
        },
    }


def read_csv(spark, input_path: str, separator: str):
    # tudo como string aqui, tipagem fica pra silver
    # escape='"' foi necessario: os csv do kaggle escapam aspas duplicando ("") e sem isso
    # uns 100-300 campos por arquivo quebravam no parse
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("sep", separator)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "false")
        .option("encoding", "utf-8")
        .load(input_path)
    )


def sanitize_column_names(df):
    # A ideia original era manter os nomes 100% raw na bronze, mas o CREATE TABLE
    # falha no hive metastore se o nome da coluna tem virgula (e o csv de 2023 vem
    # com TODAS as colunas no formato "('P2_f ', 'Cargo Atual')"). Entao troco só o
    # que é inválido por "_" e guardo o mapeamento original->bronze num json pra silver.
    # Sem lowercase e sem tirar acento de propósito - renomear de verdade é na silver.
    #   "('P2_f ', 'Cargo Atual')" -> "P2_f_Cargo_Atual"
    logger = configure_logging()

    mapping = {}
    used = set()
    for original in df.columns:
        clean = re.sub(r"[^\w]", "_", original, flags=re.UNICODE)
        clean = re.sub(r"_+", "_", clean).strip("_")
        if not clean:
            clean = "coluna"
        # se duas colunas colidirem depois da limpeza, sufixo _2, _3... (não aconteceu nas 3 bases, mas fica a garantia)
        candidate = clean
        suffix = 2
        while candidate in used:
            candidate = f"{clean}_{suffix}"
            suffix += 1
        used.add(candidate)
        mapping[original] = candidate

    df_renomeado = df.toDF(*[mapping[c] for c in df.columns])

    alteradas = sum(1 for k, v in mapping.items() if k != v)
    logger.info("Colunas sanitizadas: %s de %s tiveram o nome ajustado", alteradas, len(mapping))
    return df_renomeado, mapping


def save_column_mapping(bucket_name: str, metadata_key: str, ano_pesquisa: str, mapping: dict):
    # json original -> bronze, a silver usa isso pra achar as colunas
    logger = configure_logging()

    body = json.dumps(
        {"ano_pesquisa": ano_pesquisa, "total_colunas": len(mapping), "colunas": mapping},
        ensure_ascii=False,
        indent=2,
    )
    boto3.client("s3").put_object(
        Bucket=bucket_name,
        Key=metadata_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("mapeamento de colunas salvo em s3://%s/%s", bucket_name, metadata_key)


def write_parquet_partitioned(spark, df, output_path: str, partition_value: str):
    """Escreve dados em Parquet com particionamento por ano_pesquisa"""
    logger = configure_logging()

    # Adicionar coluna de partição temporariamente para o write
    df_com_particao = df.withColumn("ano_pesquisa", lit(partition_value))

    # Escrever com particionamento - a coluna ano_pesquisa ficará no caminho do diretório
    df_com_particao.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("ano_pesquisa") \
        .parquet(output_path)

    logger.info("Dados particionados salvos: ano_pesquisa=%s em %s", partition_value, output_path)


def register_table(spark, database: str, table_name: str, output_path: str):
    """Registra tabela no Glue Catalog com detecção automática de partições"""
    logger = configure_logging()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Criar tabela apontando para o local dos dados (sem PARTITIONED BY)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name}
        USING parquet
        LOCATION '{output_path}'
        """
    )

    # Detectar automaticamente as partições criadas pelo Spark
    spark.sql(f"MSCK REPAIR TABLE {database}.{table_name}")
    logger.info("Tabela %s registrada com partições detectadas automaticamente", table_name)


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

        df = read_csv(
            spark=spark,
            input_path=table_cfg["input_path"],
            separator=table_cfg["separator"],
        )

        logger.info("Registros lidos: %s | Colunas: %s", df.count(), len(df.columns))

        df, mapping = sanitize_column_names(df)
        save_column_mapping(
            bucket_name=bucket_name,
            metadata_key=table_cfg["metadata_key"],
            ano_pesquisa=table_cfg["ano_pesquisa"],
            mapping=mapping,
        )

        partition_value = table_cfg["ano_pesquisa"]
        write_parquet_partitioned(spark, df, table_cfg["output_path"], partition_value)
        logger.info("Parquet gravado em: %s (particionado por ano_pesquisa=%s)", table_cfg["output_path"], partition_value)

        register_table(
            spark=spark,
            database=table_cfg["database"],
            table_name=table_cfg["table_name"],
            output_path=table_cfg["output_path"],
        )
        logger.info("Tabela registrada no Glue Catalog: %s", table_cfg["table_name"])

    logger.info("Job concluído com sucesso.")
    job.commit()


if __name__ == "__main__":
    main()
