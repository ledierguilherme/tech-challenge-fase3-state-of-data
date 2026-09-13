# Exporta as tabelas gold pra csv local (pra levar pra análise). Sem spark: roda SELECT * no athena
# e baixa o csv que ele mesmo gera em athena-logs/<query-id>.csv

from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import csv
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")

AWS_REGION = "us-east-1"
DATABASE = "workspace"
EXPORT_DIR = Path(__file__).resolve().parents[1] / "exports"   # raiz do projeto/exports

TABELAS = [
    "tb_gold_perfil_mercado",
    "tb_gold_salario_por_perfil",
    "tb_gold_diversidade",
    "tb_gold_adocao_tecnologia",
    "tb_gold_adocao_ia_uso",
    "tb_gold_adocao_ia_pessoal",
    "tb_gold_barreiras_ia",
    "tb_gold_regiao_senioridade_modelo",
]

print("📋 Validando credenciais AWS...")
try:
    account_id = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()["Account"]
    print(f"   ✅ Autenticado como: {account_id}")
except Exception as e:
    print(f"   ❌ Erro de autenticação: {str(e)}")
    sys.exit(1)

BUCKET_NAME = f"{account_id}-lab"
ATHENA_PREFIX = "athena-logs/"
ATHENA_OUTPUT = f"s3://{BUCKET_NAME}/{ATHENA_PREFIX}"

s3 = boto3.client("s3", region_name=AWS_REGION)
athena = boto3.client("athena", region_name=AWS_REGION)


def run_athena_query(query: str, max_wait_time: int = 180) -> str:
    # devolve o id da query - o resultado fica em <ATHENA_OUTPUT><id>.csv
    execution_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )["QueryExecutionId"]

    elapsed_time = 0
    check_interval = 2
    while elapsed_time < max_wait_time:
        status = athena.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]["Status"]
        state = status["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if state != "SUCCEEDED":
                raise RuntimeError(f"Query {state}: {status.get('StateChangeReason', 'sem detalhes')}")
            return execution_id
        time.sleep(check_interval)
        elapsed_time += check_interval
    raise TimeoutError(f"Query não finalizou em {max_wait_time}s (id={execution_id})")


def conferir_csv(caminho: Path) -> dict:
    # confere utf-8, vírgula e conta as linhas (com csv.reader, não com wc -l, por causa das aspas)
    raw = caminho.read_bytes()
    raw.decode("utf-8")  # explode se não for utf-8
    with caminho.open(encoding="utf-8", newline="") as f:
        cabecalho = f.readline()
        f.seek(0)
        leitor = csv.reader(f)
        colunas = next(leitor)
        linhas = sum(1 for _ in leitor)
    return {
        "linhas": linhas,
        "colunas": len(colunas),
        "separador_virgula": ("," in cabecalho) and (";" not in cabecalho.replace('"', "")),
        "bom": raw.startswith(b"\xef\xbb\xbf"),
        "tamanho_kb": len(raw) / 1024,
    }


EXPORT_DIR.mkdir(parents=True, exist_ok=True)
print(f"\n📤 Exportando {len(TABELAS)} tabelas de {DATABASE} para {EXPORT_DIR}\n")

resultados = {}
falhas = []
for tabela in TABELAS:
    destino = EXPORT_DIR / f"{tabela}.csv"
    try:
        print(f"   ⏳ {tabela}...", end=" ", flush=True)
        execution_id = run_athena_query(f"SELECT * FROM {DATABASE}.{tabela}")
        chave = f"{ATHENA_PREFIX}{execution_id}.csv"
        s3.download_file(BUCKET_NAME, chave, str(destino))
        info = conferir_csv(destino)
        resultados[tabela] = info
        print(f"✅ {info['linhas']} linhas x {info['colunas']} colunas ({info['tamanho_kb']:.1f} KB)  <- s3://{BUCKET_NAME}/{chave}")
    except (ClientError, RuntimeError, TimeoutError, UnicodeDecodeError) as e:
        print(f"❌ {str(e)}")
        falhas.append(tabela)

# Resumo

print(f"\n{'='*70}")
print(f"📊 RESUMO DA EXPORTAÇÃO")
print(f"{'='*70}")
print(f"   {'tabela':<38} {'linhas':>6}  {'colunas':>7}  UTF-8  vírgula  BOM")
for tabela, info in resultados.items():
    print(f"   {tabela:<38} {info['linhas']:>6}  {info['colunas']:>7}  {'sim':<5}  {'sim' if info['separador_virgula'] else 'NÃO':<7}  {'sim' if info['bom'] else 'não'}")
print(f"\n   Arquivos em: {EXPORT_DIR}")
if falhas:
    print(f"   ❌ Falhas: {falhas}")
    print(f"{'='*70}\n")
    sys.exit(1)
print("   ✅ Todos os CSVs exportados (dialeto do Athena: UTF-8 sem BOM, separador vírgula, campos entre aspas)")
print(f"{'='*70}\n")
