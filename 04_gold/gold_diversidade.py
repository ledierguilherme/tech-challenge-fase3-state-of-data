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
        "tb_gold_diversidade": {
            "input_table": "workspace.tb_state_of_data_silver",
            "output_path": f"s3://{bucket_name}/data-output/gold/state-of-data/diversidade/",
            "metadata_key": "data-output/gold/_metadata/tb_gold_diversidade.json",
            "table_name": "tb_gold_diversidade",
            "database": "workspace",
            "partition_column": "ano_pesquisa",
            "recortes": ["genero", "cor_raca_etnia"],
            "coluna_senioridade": "nivel_senioridade_agrupada",
            "rotulo_todos": "Todos",
            "coluna_salario": "faixa_salarial_ponto_medio",
            "ordenacao": ["recorte", "nivel_senioridade_agrupada", "ano_pesquisa", "valor_recorte"],
            "builder": lambda spark, df, cfg, bucket: agregar_diversidade(df, cfg),
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

def agregar_diversidade(df, cfg: dict):
    logger = configure_logging()
    part = cfg["partition_column"]
    sen = cfg["coluna_senioridade"]
    sal = cfg["coluna_salario"]
    todos = cfg["rotulo_todos"]

    df_out = None
    for recorte in cfg["recortes"]:
        df_r = df.select(F.col(recorte).alias("valor_recorte"), F.col(sen).alias(sen), F.col(part), F.col(sal))
        df_r = rotular_nulos(df_r, ["valor_recorte", sen])

        # por senioridade + uma linha 'Todos' sem a quebra
        df_por_sen = df_r.groupBy(sen, "valor_recorte", part).agg(F.count("*").alias("contagem"), F.avg(sal).alias("salario_medio"))
        df_todos = (
            df_r.groupBy("valor_recorte", part).agg(F.count("*").alias("contagem"), F.avg(sal).alias("salario_medio"))
            .withColumn(sen, F.lit(todos))
        )
        df_agg = df_por_sen.unionByName(df_todos)

        # participação dentro do grupo (senioridade, ano). Senioridade 'Não informado' é um grupo normal aqui
        # (é quem não atua em dados); já valor_recorte 'Não informado' fica fora do denominador
        df_den = df_agg.filter(F.col("valor_recorte") != NAO_INFORMADO).groupBy(sen, part).agg(F.sum("contagem").alias("denominador"))
        df_agg = (
            df_agg.join(df_den, on=[sen, part], how="left")
            .withColumn("percentual", F.when(F.col("valor_recorte") == NAO_INFORMADO, F.lit(None)).otherwise(F.col("contagem") / F.col("denominador")).cast(DoubleType()))
            .withColumn("salario_medio", F.when(F.col("valor_recorte") == NAO_INFORMADO, F.lit(None)).otherwise(F.col("salario_medio")).cast(DoubleType()))
            .drop("denominador")
            .withColumn("recorte", F.lit(recorte))
        )
        df_out = df_agg if df_out is None else df_out.unionByName(df_agg)

    df_out = flag_amostra_baixa(df_out, "contagem")
    df_out = df_out.select(
        F.col("recorte").cast(StringType()),
        F.col("valor_recorte").cast(StringType()),
        F.col(sen).cast(StringType()),
        F.col(part).cast(IntegerType()),
        F.col("contagem").cast(LongType()),
        F.col("percentual"),
        F.col("salario_medio"),
        F.col("amostra_baixa"),
    )

    drift = []
    for recorte in cfg["recortes"]:
        drift += detectar_drift(df_out.filter(F.col("recorte") == recorte).withColumnRenamed("valor_recorte", recorte), recorte, part)
    drift += detectar_drift(df_out.filter(F.col(sen) != todos), sen, part)
    detalhes = {
        "drift": drift,
        "amostra_baixa_casos": casos_amostra_baixa(df_out, ["recorte", "valor_recorte", sen, part], "contagem"),
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
        "pergunta_respondida": "Como gênero e cor/raça se distribuem por nível de senioridade (e no total) e com que salário médio, por edição?",
        "particao": table_cfg["partition_column"],
        "qtd_linhas": qtd_linhas,
        "schema": [
            {"coluna": "recorte", "tipo": "string", "descricao": "'genero' ou 'cor_raca_etnia'"},
            {"coluna": "valor_recorte", "tipo": "string", "descricao": "categoria do recorte; NULL -> 'Não informado'"},
            {"coluna": "nivel_senioridade_agrupada", "tipo": "string", "descricao": "Júnior/Pleno/Sênior, 'Não informado' ou 'Todos' (agregado sem quebra)"},
            {"coluna": "ano_pesquisa", "tipo": "int", "descricao": "edição (partição)"},
            {"coluna": "contagem", "tipo": "bigint", "descricao": "respondentes na célula"},
            {"coluna": "percentual", "tipo": "double", "descricao": "contagem / soma dos valores regulares do recorte dentro de (senioridade, ano); NULL para 'Não informado'"},
            {"coluna": "salario_medio", "tipo": "double", "descricao": "média de faixa_salarial_ponto_medio (só não nulos); NULL para valor 'Não informado'"},
            {"coluna": "amostra_baixa", "tipo": "boolean", "descricao": f"contagem < {LIMITE_AMOSTRA_BAIXA}"},
        ],
        "regras": {
            "todos": "linha adicional por (recorte, valor_recorte, ano) com nivel_senioridade_agrupada = 'Todos'",
            "denominador": "só valor_recorte regular (exclui 'Não informado'); grupo senioridade 'Não informado' é um grupo regular (quem não atua em dados)",
            "amostra_baixa": f"contagem < {LIMITE_AMOSTRA_BAIXA}",
        },
        "validacao_esperada": "para nivel_senioridade_agrupada='Todos', SUM(percentual) por (recorte, ano) = 1.0",
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
