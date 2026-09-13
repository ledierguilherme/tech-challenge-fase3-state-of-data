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
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType


def configure_logging() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger


def build_config(bucket_name: str) -> dict:
    return {
        "tb_gold_perfil_mercado": {
            "input_table": "workspace.tb_state_of_data_silver",
            "output_path": f"s3://{bucket_name}/data-output/gold/state-of-data/perfil_mercado/",
            "metadata_key": "data-output/gold/_metadata/tb_gold_perfil_mercado.json",
            "table_name": "tb_gold_perfil_mercado",
            "database": "workspace",
            "partition_column": "ano_pesquisa",
            # rótulos das categorias especiais - essas linhas têm contagem mas percentual NULL
            "rotulo_nao_informado": "Não informado",
            "rotulo_outros": "Outros",
            "rotulo_nao_perguntado": "Não perguntado em {ano}",
            # formato longo: cada dimensão vira suas próprias linhas (dimensao, valor, ano), não é cross-tab
            "dimensoes": [
                "cargo_atual_agrupado",       # versão harmonizada (Engenheiro/Arquiteto de Dados unificados)
                "nivel_senioridade_agrupada",
                "setor",
                "numero_funcionarios",
                "modelo_trabalho_atual",
            ],
            # top-N por dimensão (None = sem corte). Cargo ficou em 14 de propósito: com 15 entrava
            # 'Analista de Inteligência de Mercado', que só existiu no questionário de 2023 e zerava
            # nos outros anos. Setor tem 21 categorias estáveis, não vale cortar.
            "top_n": {
                "cargo_atual_agrupado": 14,   # 14: deixa fora as 3 opções que só existiram em 2023 (MI, DBA, Economista) -> "Outros"
                "nivel_senioridade_agrupada": None,
                "setor": None,
                "numero_funcionarios": None,
                "modelo_trabalho_atual": None,
            },
            # perguntas que não existem em algum ano -> linha 'Não perguntado em <ano>' pra não confundir com NULL
            "anos_nao_perguntado": {
                "modelo_trabalho_atual": [2023],
            },
        },
    }


def read_silver(spark, input_table: str):
    return spark.table(input_table)


def totais_por_ano(df, partition_column: str) -> dict:
    # total de respondentes por ano (usado na linha 'Não perguntado')
    return {r[partition_column]: r["qtd"] for r in df.groupBy(partition_column).agg(F.count("*").alias("qtd")).collect()}


def selecionar_top_n(df, coluna: str, top_n: int) -> list:
    # top-N pelo volume somado dos 3 anos, calculado nos dados mesmo (nada hardcoded)
    rows = (
        df.filter(F.col(coluna).isNotNull())
        .groupBy(coluna).agg(F.count("*").alias("qtd"))
        .orderBy(F.desc("qtd"), F.asc(coluna))
        .limit(top_n)
        .collect()
    )
    return [r[coluna] for r in rows]


def agregar_dimensao(df, coluna: str, cfg: dict, totais: dict):
    # Regras que valem pra todas as dimensões:
    #  - NULL vira 'Não informado', com contagem mas percentual NULL e fora do denominador
    #    (no cargo são ~27% que não trabalham com dados - se fossem pro 'Outros' virariam a maior categoria)
    #  - com top-N, o que fica fora vai pra 'Outros' (esse entra no denominador)
    #  - percentual = contagem / soma das categorias regulares daquele (dimensao, ano)
    logger = configure_logging()
    part = cfg["partition_column"]
    nao_informado = cfg["rotulo_nao_informado"]
    outros = cfg["rotulo_outros"]
    top_n = cfg["top_n"].get(coluna)
    anos_nao_perguntado = cfg["anos_nao_perguntado"].get(coluna, [])
    detalhes = {"coluna": coluna, "top_n": top_n, "top_n_aplicado": False, "top_n_valores": None, "anos_nao_perguntado": anos_nao_perguntado}

    df_dim = df.select(F.col(coluna).alias("valor"), F.col(part).alias(part))
    if anos_nao_perguntado:
        df_dim = df_dim.filter(~F.col(part).isin(anos_nao_perguntado))

    cardinalidade = df_dim.filter(F.col("valor").isNotNull()).select("valor").distinct().count()
    detalhes["cardinalidade"] = cardinalidade
    logger.info("[%s] cardinalidade (valores distintos não nulos): %s", coluna, cardinalidade)

    if top_n is None:
        logger.info("[%s] sem top-N: todas as %s categorias mantidas (sem 'Outros')", coluna, cardinalidade)

    if top_n is not None:
        top_valores = selecionar_top_n(df_dim, "valor", top_n)
        detalhes["top_n_aplicado"] = True
        detalhes["top_n_valores"] = top_valores
        logger.info("[%s] top-%s por volume total (3 anos): %s", coluna, top_n, top_valores)
        valor_expr = (
            F.when(F.col("valor").isNull(), F.lit(nao_informado))
             .when(F.col("valor").isin(top_valores), F.col("valor"))
             .otherwise(F.lit(outros))
        )
    else:
        valor_expr = F.coalesce(F.col("valor"), F.lit(nao_informado))

    df_agg = (
        df_dim.withColumn("valor", valor_expr)
        .groupBy(part, "valor")
        .agg(F.count("*").alias("contagem"))
    )

    # denominador só com categorias regulares
    df_den = (
        df_agg.filter(F.col("valor") != nao_informado)
        .groupBy(part).agg(F.sum("contagem").alias("denominador"))
    )
    df_agg = (
        df_agg.join(df_den, on=part, how="left")
        .withColumn(
            "percentual",
            F.when(F.col("valor") == nao_informado, F.lit(None).cast(DoubleType()))
             .otherwise(F.col("contagem") / F.col("denominador"))
        )
        .drop("denominador")
    )

    # linha 'Não perguntado em <ano>' (schema explícito porque a coluna percentual é toda None)
    if anos_nao_perguntado:
        linhas = [
            (cfg["rotulo_nao_perguntado"].format(ano=ano), ano, totais[ano], None)
            for ano in anos_nao_perguntado
        ]
        schema_np = StructType([
            StructField("valor", StringType(), False),
            StructField(part, IntegerType(), False),
            StructField("contagem", LongType(), False),
            StructField("percentual", DoubleType(), True),
        ])
        df_np = df.sparkSession.createDataFrame(linhas, schema_np)
        df_agg = df_agg.unionByName(df_np)

    df_out = df_agg.select(
        F.lit(coluna).cast(StringType()).alias("dimensao"),
        F.col("valor").cast(StringType()).alias("valor"),
        F.col(part).cast(IntegerType()).alias(part),
        F.col("contagem").cast(LongType()).alias("contagem"),
        F.col("percentual").cast(DoubleType()).alias("percentual"),
    )

    # quanto foi parar em Outros / Não informado, pro log e pros metadados
    agrupados = df_out.filter(F.col("valor").isin(outros, nao_informado)).groupBy("valor", part).agg(F.sum("contagem").alias("qtd")).collect()
    detalhes["agrupados"] = {f"{r['valor']}|{r[part]}": r["qtd"] for r in agrupados}
    for r in sorted(agrupados, key=lambda x: (x["valor"], x[part])):
        logger.info("[%s] %s em %s: %s registros", coluna, r["valor"], r[part], r["qtd"])

    return df_out, detalhes


def write_parquet_partitioned(spark, df, output_path: str, partition_column: str):
    # repartition pela partição pra sair 1 arquivo por ano (na silver saíram 60 arquivos de 16KB por partição...)
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

    # drop antes de recriar - os dados já foram sobrescritos, aqui é só o catálogo
    spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")

    # mesmo padrão da bronze: CREATE TABLE ... USING parquet LOCATION + MSCK, sem crawler
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name}
        USING parquet
        LOCATION '{output_path}'
        """
    )

    spark.sql(f"MSCK REPAIR TABLE {database}.{table_name}")
    logger.info("Tabela %s registrada com partições detectadas automaticamente", table_name)


def save_metadata(bucket_name: str, metadata_key: str, table_cfg: dict, detalhes_dimensoes: list, qtd_linhas: int):
    # json com schema, regras e o top-N que foi calculado (pra conseguir explicar depois o que entrou em 'Outros')
    logger = configure_logging()

    metadata = {
        "tabela": f"{table_cfg['database']}.{table_cfg['table_name']}",
        "location": table_cfg["output_path"],
        "origem": table_cfg["input_table"],
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "pergunta_respondida": "Perfil do mercado: distribuição de cargo, senioridade, setor, porte da empresa e modelo de trabalho por edição",
        "formato": "longo (tidy): uma linha por (dimensao, valor, ano_pesquisa)",
        "particao": table_cfg["partition_column"],
        "qtd_linhas": qtd_linhas,
        "schema": [
            {"coluna": "dimensao", "tipo": "string", "descricao": "nome da dimensão (coluna da Silver)"},
            {"coluna": "valor", "tipo": "string", "descricao": "categoria dentro da dimensão"},
            {"coluna": "ano_pesquisa", "tipo": "int", "descricao": "edição da pesquisa (partição)"},
            {"coluna": "contagem", "tipo": "bigint", "descricao": "respondentes na categoria"},
            {"coluna": "percentual", "tipo": "double", "descricao": "contagem / soma das categorias regulares de (dimensao, ano); NULL para 'Não informado' e 'Não perguntado em <ano>'"},
        ],
        "regras": {
            "nao_informado": f"valor NULL -> '{table_cfg['rotulo_nao_informado']}' (contagem preenchida, percentual NULL, fora do denominador)",
            "outros": f"com top-N: valores fora do top-N -> '{table_cfg['rotulo_outros']}' (entra no denominador; NULL continua 'Não informado')",
            "nao_perguntado": f"anos sem a pergunta -> 1 linha '{table_cfg['rotulo_nao_perguntado']}' com contagem = total de respondentes do ano e percentual NULL",
        },
        "top_n_config": table_cfg["top_n"],
        "dimensoes": detalhes_dimensoes,
    }
    boto3.client("s3").put_object(
        Bucket=bucket_name,
        Key=metadata_key,
        Body=json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Metadados salvos em: s3://%s/%s", bucket_name, metadata_key)


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
        totais = totais_por_ano(df_silver, table_cfg["partition_column"])
        logger.info("Registros lidos da Silver: %s | por ano: %s", sum(totais.values()), totais)

        df_gold = None
        detalhes_dimensoes = []
        for coluna in table_cfg["dimensoes"]:
            logger.info("Agregando dimensão: %s (top_n=%s)", coluna, table_cfg["top_n"].get(coluna))
            df_dim, detalhes = agregar_dimensao(df_silver, coluna, table_cfg, totais)
            detalhes_dimensoes.append(detalhes)
            df_gold = df_dim if df_gold is None else df_gold.unionByName(df_dim)

        df_gold = df_gold.orderBy("dimensao", table_cfg["partition_column"], F.desc("contagem"))
        qtd_linhas = df_gold.count()
        logger.info("Linhas na Gold: %s", qtd_linhas)

        write_parquet_partitioned(spark, df_gold, table_cfg["output_path"], table_cfg["partition_column"])
        logger.info("Parquet gravado em: %s", table_cfg["output_path"])

        register_table(
            spark=spark,
            database=table_cfg["database"],
            table_name=table_cfg["table_name"],
            output_path=table_cfg["output_path"],
        )
        logger.info("Tabela registrada no Glue Catalog: %s", table_cfg["table_name"])

        save_metadata(bucket_name, table_cfg["metadata_key"], table_cfg, detalhes_dimensoes, qtd_linhas)

    logger.info("Job concluído com sucesso.")
    job.commit()


if __name__ == "__main__":
    main()
