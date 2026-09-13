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
        # (a) uso de IA na empresa / pelo indivíduo
        "tb_gold_adocao_ia_uso": {
            "input_table": "workspace.tb_state_of_data_silver",
            "output_path": f"s3://{bucket_name}/data-output/gold/state-of-data/adocao_ia_uso/",
            "metadata_key": "data-output/gold/_metadata/tb_gold_adocao_ia_uso.json",
            "table_name": "tb_gold_adocao_ia_uso",
            "database": "workspace",
            "partition_column": "ano_pesquisa",
            "blocos": {"empresa": "ia_emp_", "individuo": "ia_ind_"},
            "excluir_colunas": ["ia_emp_bons_resultados_llm"],   # é string, não booleano
            "excluir_prefixos": ["ia_emp_motivo_"],              # os motivos ficam na tb_gold_barreiras_ia
            "anos": [2023, 2024, 2025],
            "ordenacao": ["bloco", "ano_pesquisa", "item"],
            "builder": lambda spark, df, cfg, bucket: agregar_adocao_ia_uso(spark, df, cfg),
        },
        # (b) nível de uso pessoal x senioridade. Fiz 2 tabelas em vez de union porque as chaves e as
        # métricas são diferentes - juntar ia obrigar um monte de coluna NULL e confundir no athena
        "tb_gold_adocao_ia_pessoal": {
            "input_table": "workspace.tb_state_of_data_silver",
            "output_path": f"s3://{bucket_name}/data-output/gold/state-of-data/adocao_ia_pessoal/",
            "metadata_key": "data-output/gold/_metadata/tb_gold_adocao_ia_pessoal.json",
            "table_name": "tb_gold_adocao_ia_pessoal",
            "database": "workspace",
            "partition_column": "ano_pesquisa",
            "coluna_senioridade": "nivel_senioridade_agrupada",
            "coluna_nivel_uso": "ia_pessoal_nivel_uso",
            "ordenacao": ["nivel_senioridade_agrupada", "ano_pesquisa", "nivel_uso"],
            "builder": lambda spark, df, cfg, bucket: agregar_adocao_ia_pessoal(df, cfg),
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
    # agrega tudo num agg só e monta as linhas em python (poucas linhas)
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
    # schema explícito senão o spark não infere coluna toda None
    schema = StructType([StructField(n, t, True) for n, t in campos])
    return spark.createDataFrame([tuple(l[n] for n, _ in campos) for l in linhas], schema)


# ---- lógica das tabelas

def agregar_adocao_ia_uso(spark, df, cfg: dict):
    logger = configure_logging()
    part = cfg["partition_column"]
    linhas = []
    for bloco, prefixo in cfg["blocos"].items():
        cols = sorted(
            c for c in df.columns
            if c.startswith(prefixo) and c not in cfg["excluir_colunas"] and not any(c.startswith(p) for p in cfg["excluir_prefixos"])
        )
        for l in totais_booleanos(spark, df, cols, part, cfg["anos"]):
            linhas.append({"bloco": bloco, "item": l["coluna"][len(prefixo):], "fonte": l["coluna"], part: l["ano"],
                           "contagem_true": l["contagem_true"], "total_respondentes": l["total_respondentes"]})

    campos = [("bloco", StringType()), ("item", StringType()), ("fonte", StringType()), (part, IntegerType()),
              ("contagem_true", LongType()), ("total_respondentes", LongType())]
    df_out = montar_df_tidy(spark, linhas, campos)
    df_out = df_out.withColumn("percentual", F.when(F.col("total_respondentes") > 0, F.col("contagem_true") / F.col("total_respondentes")).cast(DoubleType()))
    df_out = flag_amostra_baixa(df_out, "contagem_true")

    drift = []
    for bloco in cfg["blocos"]:
        drift += [dict(d, bloco=bloco) for d in detectar_drift(df_out.filter((F.col("bloco") == bloco) & (F.col("total_respondentes") > 0)), "item", part)]
    detalhes = {
        "drift": drift,
        "amostra_baixa_casos": casos_amostra_baixa(df_out, ["bloco", "item", part], "contagem_true"),
        "referencias": {l["fonte"] + "|" + str(l[part]): {"contagem_true": l["contagem_true"], "total_respondentes": l["total_respondentes"]}
                        for l in linhas if l["fonte"] in ("ia_emp_uso_independente", "ia_ind_uso_independente", "ia_ind_copilots_dev")},
    }
    for d in drift:
        logger.warning("DRIFT %s", d)
    return df_out, detalhes


def agregar_adocao_ia_pessoal(df, cfg: dict):
    logger = configure_logging()
    part = cfg["partition_column"]
    sen = cfg["coluna_senioridade"]

    df_p = df.select(F.col(sen), F.col(cfg["coluna_nivel_uso"]).alias("nivel_uso"), F.col(part))
    df_p = rotular_nulos(df_p, [sen, "nivel_uso"])
    df_agg = df_p.groupBy(sen, "nivel_uso", part).agg(F.count("*").alias("contagem"))
    df_den = df_agg.filter(F.col("nivel_uso") != NAO_INFORMADO).groupBy(sen, part).agg(F.sum("contagem").alias("denominador"))
    df_agg = (
        df_agg.join(df_den, on=[sen, part], how="left")
        .withColumn("percentual", F.when(F.col("nivel_uso") == NAO_INFORMADO, F.lit(None)).otherwise(F.col("contagem") / F.col("denominador")).cast(DoubleType()))
        .drop("denominador")
    )
    df_agg = flag_amostra_baixa(df_agg, "contagem")
    df_out = df_agg.select(
        F.col(sen).cast(StringType()), F.col("nivel_uso").cast(StringType()), F.col(part).cast(IntegerType()),
        F.col("contagem").cast(LongType()), F.col("percentual"), F.col("amostra_baixa"),
    )
    drift = detectar_drift(df_out, "nivel_uso", part) + detectar_drift(df_out, sen, part)
    respondentes = {str(r[part]): r["qtd"] for r in df_out.filter(F.col("nivel_uso") != NAO_INFORMADO).groupBy(part).agg(F.sum("contagem").alias("qtd")).collect()}
    detalhes = {
        "drift": drift,
        "amostra_baixa_casos": casos_amostra_baixa(df_out, [sen, "nivel_uso", part], "contagem"),
        "referencias": {"respondentes_nivel_uso_por_ano": respondentes},
    }
    for d in drift:
        logger.warning("DRIFT %s", d)
    return df_out, detalhes


def build_metadata(table_cfg: dict, detalhes: dict, qtd_linhas: int) -> dict:
    comum = {
        "tabela": f"{table_cfg['database']}.{table_cfg['table_name']}",
        "location": table_cfg["output_path"],
        "origem": table_cfg["input_table"],
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "particao": table_cfg["partition_column"],
        "qtd_linhas": qtd_linhas,
        "decisao_2_tabelas": "As partes (a) e (b) têm chaves e métricas diferentes (item × contagem_true/total vs senioridade × nivel_uso × contagem/percentual); um union exigiria colunas NULL-padded e confundiria consultas no Athena. Duas tabelas no mesmo job é mais simples.",
    }
    if table_cfg["table_name"] == "tb_gold_adocao_ia_uso":
        return {**comum,
            "pergunta_respondida": "Como as empresas usam IA generativa (visão da empresa e do indivíduo) e como isso evolui por edição?",
            "schema": [
                {"coluna": "bloco", "tipo": "string", "descricao": "'empresa' (ia_emp_*) ou 'individuo' (ia_ind_*)"},
                {"coluna": "item", "tipo": "string", "descricao": "sufixo da coluna (uso_independente, direcionamento_central, copilots_dev, ...)"},
                {"coluna": "fonte", "tipo": "string", "descricao": "coluna da Silver"},
                {"coluna": "ano_pesquisa", "tipo": "int", "descricao": "edição (partição)"},
                {"coluna": "contagem_true", "tipo": "bigint", "descricao": "respondentes com true"},
                {"coluna": "total_respondentes", "tipo": "bigint", "descricao": "não nulos na coluna/ano (quem passou pela pergunta)"},
                {"coluna": "percentual", "tipo": "double", "descricao": "contagem_true / total_respondentes"},
                {"coluna": "amostra_baixa", "tipo": "boolean", "descricao": f"contagem_true < {LIMITE_AMOSTRA_BAIXA}"},
            ],
            "regras": {"excluidos": "ia_emp_motivo_* (tb_gold_barreiras_ia) e ia_emp_bons_resultados_llm (categórica)"},
            "validacao_esperada": "ia_emp_uso_independente true 303/395/312; ia_ind_uso_independente 1723/1811/1017; ia_ind_copilots_dev 578/948/685",
            **detalhes}
    return {**comum,
        "pergunta_respondida": "Qual o nível de uso pessoal de IA generativa (não usa / gratuito-copilot / paga / empresa paga) por senioridade e edição?",
        "schema": [
            {"coluna": "nivel_senioridade_agrupada", "tipo": "string", "descricao": "Júnior/Pleno/Sênior; NULL -> 'Não informado'"},
            {"coluna": "nivel_uso", "tipo": "string", "descricao": "ia_pessoal_nivel_uso; NULL -> 'Não informado'"},
            {"coluna": "ano_pesquisa", "tipo": "int", "descricao": "edição (partição)"},
            {"coluna": "contagem", "tipo": "bigint", "descricao": "respondentes na célula"},
            {"coluna": "percentual", "tipo": "double", "descricao": "contagem / soma dos níveis regulares dentro de (senioridade, ano); NULL para 'Não informado'"},
            {"coluna": "amostra_baixa", "tipo": "boolean", "descricao": f"contagem < {LIMITE_AMOSTRA_BAIXA}"},
        ],
        "regras": {"denominador": "exclui nivel_uso 'Não informado'; senioridade 'Não informado' é grupo regular"},
        "validacao_esperada": "respondentes com nivel_uso por ano = 3772/3619/2106",
        **detalhes}


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
