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
        "tb_gold_adocao_tecnologia": {
            "input_table": "workspace.tb_state_of_data_silver",
            "output_path": f"s3://{bucket_name}/data-output/gold/state-of-data/adocao_tecnologia/",
            "metadata_key": "data-output/gold/_metadata/tb_gold_adocao_tecnologia.json",
            "table_name": "tb_gold_adocao_tecnologia",
            "database": "workspace",
            "partition_column": "ano_pesquisa",
            # tec_usa_* só existe em 2023/2024 (em 2025 a pergunta de uso no dia a dia foi removida)
            "anos_uso": [2023, 2024],
            # 'preferida': em 2025 são booleanos (multi-select), em 2023/2024 é texto livre normalizado (resposta única)
            # -> fontes de natureza diferente, por isso a coluna 'fonte' na saída
            "anos_pref_bool": [2025],
            "anos_pref_normalizada": [2023, 2024],
            "top_n_pref_normalizada": 10,
            # nomes do texto livre -> mesmo id dos sufixos tec_usa_/tec_pref_
            "mapa_pref_normalizada": {
                "Python": "python", "SQL": "sql", "R": "r", "Scala": "scala", "Rust": "rust", "Julia": "julia",
                "C/C++/C#": "c_cpp_csharp", "Java": "java", "JavaScript": "javascript", "Go": "go", "DAX/M": "dax",
                "Outra": "outra", "Não informado": "nao_informado",
            },
            "ordenacao": ["tipo_metrica", "ano_pesquisa", "tecnologia"],
            "builder": lambda spark, df, cfg, bucket: agregar_adocao_tecnologia(spark, df, cfg),
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
    # 1 agg só com todas as colunas e monto as linhas em python - são poucas dezenas de linhas, não compensa um unpivot no spark
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

def agregar_adocao_tecnologia(spark, df, cfg: dict):
    logger = configure_logging()
    part = cfg["partition_column"]
    linhas = []

    # 1) uso no dia a dia
    cols_usa = sorted(c for c in df.columns if c.startswith("tec_usa_"))
    for l in totais_booleanos(spark, df, cols_usa, part, cfg["anos_uso"]):
        linhas.append({"tecnologia": l["coluna"][len("tec_usa_"):], "tipo_metrica": "uso_dia_a_dia", "fonte": l["coluna"],
                       part: l["ano"], "contagem_true": l["contagem_true"], "total_respondentes": l["total_respondentes"]})

    # 2) preferida 2025 (booleanos)
    cols_pref = sorted(c for c in df.columns if c.startswith("tec_pref_") and c != "tec_pref_normalizada")
    for l in totais_booleanos(spark, df, cols_pref, part, cfg["anos_pref_bool"]):
        linhas.append({"tecnologia": l["coluna"][len("tec_pref_"):], "tipo_metrica": "preferida", "fonte": l["coluna"],
                       part: l["ano"], "contagem_true": l["contagem_true"], "total_respondentes": l["total_respondentes"]})

    # 3) preferida 2023/2024 (top-10 do texto livre)
    df_norm = df.filter(F.col(part).isin(cfg["anos_pref_normalizada"]) & F.col("tec_pref_normalizada").isNotNull())
    totais_norm = {r[part]: r["qtd"] for r in df_norm.groupBy(part).agg(F.count("*").alias("qtd")).collect()}
    top_norm = [r["tec_pref_normalizada"] for r in df_norm.groupBy("tec_pref_normalizada").agg(F.count("*").alias("qtd"))
                .orderBy(F.desc("qtd"), F.asc("tec_pref_normalizada")).limit(cfg["top_n_pref_normalizada"]).collect()]
    logger.info("Top-%s de tec_pref_normalizada (2023/2024): %s", cfg["top_n_pref_normalizada"], top_norm)
    mapa = cfg["mapa_pref_normalizada"]
    for r in df_norm.filter(F.col("tec_pref_normalizada").isin(top_norm)).groupBy(part, "tec_pref_normalizada").agg(F.count("*").alias("qtd")).collect():
        linhas.append({"tecnologia": mapa.get(r["tec_pref_normalizada"], r["tec_pref_normalizada"].lower()), "tipo_metrica": "preferida",
                       "fonte": "tec_pref_normalizada", part: r[part], "contagem_true": int(r["qtd"]), "total_respondentes": int(totais_norm[r[part]])})

    campos = [("tecnologia", StringType()), ("tipo_metrica", StringType()), ("fonte", StringType()), (part, IntegerType()),
              ("contagem_true", LongType()), ("total_respondentes", LongType())]
    df_out = montar_df_tidy(spark, linhas, campos)
    df_out = df_out.withColumn("percentual", (F.col("contagem_true") / F.col("total_respondentes")).cast(DoubleType()))
    df_out = flag_amostra_baixa(df_out, "contagem_true")

    # drift só dentro da mesma fonte, senão acusa a troca de pergunta de 2025 como drift
    drift = detectar_drift(df_out.filter(F.col("tipo_metrica") == "uso_dia_a_dia"), "tecnologia", part)
    drift += [dict(d, escopo="preferida/tec_pref_normalizada") for d in detectar_drift(df_out.filter(F.col("fonte") == "tec_pref_normalizada"), "tecnologia", part)]
    detalhes = {
        "drift": drift,
        "top_pref_normalizada": top_norm,
        "amostra_baixa_casos": casos_amostra_baixa(df_out, ["tecnologia", "tipo_metrica", "fonte", part], "contagem_true"),
        "referencias": {l["fonte"] + "|" + str(l[part]): {"contagem_true": l["contagem_true"], "total_respondentes": l["total_respondentes"]}
                        for l in linhas if l["fonte"] in ("tec_usa_python", "tec_pref_python", "tec_usa_sql")},
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
        "pergunta_respondida": "Quais linguagens são usadas no dia a dia (2023-2024) e quais são preferidas (2023-2025), e como a adoção evolui?",
        "particao": table_cfg["partition_column"],
        "qtd_linhas": qtd_linhas,
        "schema": [
            {"coluna": "tecnologia", "tipo": "string", "descricao": "sufixo de tec_usa_*/tec_pref_* ou id mapeado do texto livre (ex.: python, sql, c_cpp_csharp)"},
            {"coluna": "tipo_metrica", "tipo": "string", "descricao": "'uso_dia_a_dia' (2023/2024) ou 'preferida' (2023-2025)"},
            {"coluna": "fonte", "tipo": "string", "descricao": "coluna da Silver de origem (tec_usa_*, tec_pref_*, tec_pref_normalizada) - documenta a mudança de fonte entre edições"},
            {"coluna": "ano_pesquisa", "tipo": "int", "descricao": "edição (partição)"},
            {"coluna": "contagem_true", "tipo": "bigint", "descricao": "respondentes com true (booleanos) ou com a categoria (texto normalizado)"},
            {"coluna": "total_respondentes", "tipo": "bigint", "descricao": "não nulos na coluna/ano (booleanos) ou não nulos em tec_pref_normalizada/ano"},
            {"coluna": "percentual", "tipo": "double", "descricao": "contagem_true / total_respondentes"},
            {"coluna": "amostra_baixa", "tipo": "boolean", "descricao": f"contagem_true < {LIMITE_AMOSTRA_BAIXA}"},
        ],
        "regras": {
            "uso_dia_a_dia": "só 2023/2024 (a pergunta não existe em 2025); tec_usa_dax não existe na Silver",
            "preferida_2025": "booleanos tec_pref_* (multi-select)",
            "preferida_2023_2024": f"top-{table_cfg['top_n_pref_normalizada']} de tec_pref_normalizada (resposta única em texto livre normalizado) - fonte de natureza diferente da de 2025; coluna 'fonte' documenta",
            "nota_metodologica": "A partir de 2025 a pesquisa não repete 'linguagens usadas no dia a dia'; 2025 é reportado só como 'preferida'.",
        },
        "validacao_esperada": "tec_usa_python 2825/2935 (2023/2024), tec_pref_python 1929 (2025); totais 3772/3589/2096",
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
