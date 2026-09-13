# Validação da bronze: lista o que foi gravado no s3 e confere as tabelas no catálogo via athena

import boto3
from botocore.exceptions import ClientError
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")

# Configuração Global - Variáveis Reutilizáveis

AWS_REGION = "us-east-1"
DATABASE = "workspace"
BRONZE_PREFIX = "data-output/bronze/"
TABELAS_ESPERADAS = [
    "tb_pesquisa_2023_bronze",
    "tb_pesquisa_2024_bronze",
    "tb_pesquisa_2025_bronze",
]

print("📋 Validando credenciais AWS...")
try:
    account_id = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()["Account"]
    print(f"   ✅ Autenticado como: {account_id}")
except Exception as e:
    print(f"   ❌ Erro de autenticação: {str(e)}")
    sys.exit(1)

BUCKET_NAME = f"{account_id}-lab"
ATHENA_OUTPUT = f"s3://{BUCKET_NAME}/athena-logs/"

s3 = boto3.client("s3", region_name=AWS_REGION)
athena = boto3.client("athena", region_name=AWS_REGION)

# PASSO 7.1 - Listar objetos da Bronze no bucket (Com Formatação)

print(f"\n📊 Listando objetos da Bronze: s3://{BUCKET_NAME}/{BRONZE_PREFIX}\n")

try:
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=BRONZE_PREFIX)

    objetos = []
    for page in pages:
        objetos.extend(page.get("Contents", []))

    if not objetos:
        print("   ℹ️  Nenhum arquivo encontrado")
    else:
        objetos_por_pasta = {}
        tamanho_total = 0

        for obj in objetos:
            key = obj["Key"]
            pasta = key.rsplit("/", 1)[0] if "/" in key else "root"

            if pasta not in objetos_por_pasta:
                objetos_por_pasta[pasta] = []

            objetos_por_pasta[pasta].append({
                "nome": key.rsplit("/", 1)[1] if "/" in key else key,
                "tamanho": obj["Size"],
                "chave_completa": key,
            })

            tamanho_total += obj["Size"]

        for pasta in sorted(objetos_por_pasta.keys()):
            print(f"📁 {pasta}/")
            for arquivo in objetos_por_pasta[pasta]:
                if not arquivo["nome"]:
                    continue  # marcador de pasta
                tamanho_kb = arquivo["tamanho"] / 1024
                tamanho_mb = arquivo["tamanho"] / (1024 * 1024)

                if tamanho_mb > 1:
                    tamanho_str = f"{tamanho_mb:.2f} MB"
                else:
                    tamanho_str = f"{tamanho_kb:.2f} KB"

                print(f"   📄 {arquivo['nome']:<70} ({tamanho_str})")

        print(f"\n{'='*60}")
        print(f"📈 SUMÁRIO")
        print(f"{'='*60}")
        print(f"   Total de objetos: {len(objetos)}")
        print(f"   Tamanho total: {tamanho_total / (1024 * 1024):.2f} MB")
        print(f"{'='*60}\n")

except ClientError as e:
    print(f"❌ Erro ao listar objetos: {str(e)}")
    sys.exit(1)

# PASSO 7.2 - Consultar o Glue Catalog via Athena


def run_athena_query(query: str, database: str = None, max_wait_time: int = 120):
    # roda a query e espera terminar; devolve as linhas (a primeira é o cabeçalho)
    params = {
        "QueryString": query,
        "ResultConfiguration": {"OutputLocation": ATHENA_OUTPUT},
    }
    if database:
        params["QueryExecutionContext"] = {"Database": database}

    execution_id = athena.start_query_execution(**params)["QueryExecutionId"]

    elapsed_time = 0
    check_interval = 2
    while elapsed_time < max_wait_time:
        status = athena.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]["Status"]
        state = status["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if state != "SUCCEEDED":
                raise RuntimeError(f"Query {state}: {status.get('StateChangeReason', 'sem detalhes')}")
            break
        time.sleep(check_interval)
        elapsed_time += check_interval
    else:
        raise TimeoutError(f"Query não finalizou em {max_wait_time}s (id={execution_id})")

    rows = []
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=execution_id):
        for row in page["ResultSet"]["Rows"]:
            rows.append([col.get("VarCharValue", "") for col in row["Data"]])
    return rows


print(f"🔎 Consultando o Glue Catalog via Athena (output: {ATHENA_OUTPUT})\n")

print(f"1️⃣  SHOW TABLES IN {DATABASE}\n")
try:
    rows = run_athena_query(f"SHOW TABLES IN {DATABASE}")
    tabelas = [r[0] for r in rows if r and r[0]]
    if not tabelas:
        print("   ℹ️  Nenhuma tabela encontrada")
    for t in tabelas:
        icon = "✅" if t in TABELAS_ESPERADAS else "📄"
        print(f"   {icon} {t}")

    faltando = [t for t in TABELAS_ESPERADAS if t not in tabelas]
    if faltando:
        print(f"\n   ❌ Tabelas esperadas não encontradas: {faltando}")
        sys.exit(1)
    print(f"\n   ✅ Todas as {len(TABELAS_ESPERADAS)} tabelas Bronze estão registradas\n")
except Exception as e:
    print(f"   ❌ Erro ao consultar Athena: {str(e)}")
    sys.exit(1)

print(f"2️⃣  Contagem de registros por tabela/partição\n")
falhas = []
for tabela in TABELAS_ESPERADAS:
    try:
        rows = run_athena_query(
            f'SELECT ano_pesquisa, COUNT(*) AS qtd FROM "{DATABASE}"."{tabela}" GROUP BY ano_pesquisa',
            database=DATABASE,
        )
        for ano, qtd in rows[1:]:
            print(f"   ✅ {tabela:<28} ano_pesquisa={ano}  registros={qtd}")
    except Exception as e:
        print(f"   ❌ {tabela:<28} erro: {str(e)}")
        falhas.append(tabela)

print(f"\n{'='*60}")
if falhas:
    print(f"❌ Validação com falhas em: {falhas}")
    print(f"{'='*60}\n")
    sys.exit(1)
print("✅ Validação da camada Bronze concluída!")
print(f"{'='*60}\n")
