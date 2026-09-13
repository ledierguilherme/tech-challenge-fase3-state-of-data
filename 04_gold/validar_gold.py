# validações da tb_gold_perfil_mercado (a primeira gold, por isso tem um script só pra ela):
# somas x silver, SUM(percentual)=1, top-N escolhido, linhas, drift do cargo resolvido, setor sem corte.
# também baixa o json de metadados pra metadata/

from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import json
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")

# Configuração Global - Variáveis Reutilizáveis

AWS_REGION = "us-east-1"
DATABASE = "workspace"
TABELA_GOLD = "tb_gold_perfil_mercado"
TABELA_SILVER = "tb_state_of_data_silver"
METADATA_KEY = "data-output/gold/_metadata/tb_gold_perfil_mercado.json"
METADATA_LOCAL = Path(__file__).resolve().parent / "metadata" / "tb_gold_perfil_mercado.json"
TOLERANCIA_PERCENTUAL = 1e-6

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


def run_athena_query(query: str, database: str = DATABASE, max_wait_time: int = 120):
    execution_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
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
            break
        time.sleep(check_interval)
        elapsed_time += check_interval
    else:
        raise TimeoutError(f"Query não finalizou em {max_wait_time}s (id={execution_id})")

    rows = []
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=execution_id):
        for row in page["ResultSet"]["Rows"]:
            rows.append([col.get("VarCharValue") for col in row["Data"]])
    return rows[1:]


falhas = []

# Metadados gerados pelo job (top-N, agrupamentos, dimensões)

print(f"\n📥 Baixando metadados: s3://{BUCKET_NAME}/{METADATA_KEY}")
try:
    METADATA_LOCAL.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(BUCKET_NAME, METADATA_KEY, str(METADATA_LOCAL))
    metadata = json.loads(METADATA_LOCAL.read_text(encoding="utf-8"))
    print(f"   ✅ Salvo em: {METADATA_LOCAL}")
except ClientError as e:
    print(f"   ❌ Erro ao baixar metadados: {str(e)}")
    sys.exit(1)

dimensoes = [d["coluna"] for d in metadata["dimensoes"]]
nao_perguntado = {d["coluna"]: d.get("anos_nao_perguntado", []) for d in metadata["dimensoes"]}
print(f"   Dimensões: {dimensoes}")

# 1 - SUM(contagem) por (dimensao, ano) x respondentes não nulos na Silver

print(f"\n1️⃣  SUM(contagem) na Gold x COUNT(coluna) não nulo na Silver, por dimensão e ano\n")
gold_soma = {
    (r[0], int(r[1])): int(r[2])
    for r in run_athena_query(
        f"""SELECT dimensao, ano_pesquisa, SUM(contagem)
            FROM {DATABASE}.{TABELA_GOLD}
            WHERE valor NOT LIKE 'Não perguntado%' AND valor <> 'Não informado'
            GROUP BY 1, 2"""
    )
}
silver_soma = {}
for dim in dimensoes:
    for r in run_athena_query(f"SELECT ano_pesquisa, COUNT({dim}) FROM {DATABASE}.{TABELA_SILVER} GROUP BY 1"):
        silver_soma[(dim, int(r[0]))] = int(r[1])

print(f"   {'dimensao':<28} {'ano':<5} {'gold':>6} {'silver':>7}  status")
for (dim, ano) in sorted(silver_soma):
    if ano in nao_perguntado.get(dim, []):
        continue  # esse ano tem a linha 'Não perguntado'
    g = gold_soma.get((dim, ano), 0)
    sv = silver_soma[(dim, ano)]
    status = "✅" if g == sv else "❌"
    if g != sv:
        falhas.append(f"soma {dim}/{ano}: gold={g} silver={sv}")
    print(f"   {dim:<28} {ano:<5} {g:>6} {sv:>7}  {status}")

# 2 - SUM(percentual) por (dimensao, ano) ~= 1.0

print(f"\n2️⃣  SUM(percentual) por dimensão e ano (excluindo percentual NULL)\n")
for r in run_athena_query(
    f"""SELECT dimensao, ano_pesquisa, ROUND(SUM(percentual), 8), COUNT(percentual)
        FROM {DATABASE}.{TABELA_GOLD}
        WHERE percentual IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, 2"""
):
    dim, ano, soma, qtd = r[0], int(r[1]), float(r[2]), int(r[3])
    ok = abs(soma - 1.0) < TOLERANCIA_PERCENTUAL
    if not ok:
        falhas.append(f"percentual {dim}/{ano}: soma={soma}")
    print(f"   {dim:<28} {ano:<5} soma={soma:.8f}  ({qtd} categorias)  {'✅' if ok else '❌'}")

print(f"\n   Linhas especiais (percentual NULL):")
for r in run_athena_query(
    f"""SELECT dimensao, valor, ano_pesquisa, contagem
        FROM {DATABASE}.{TABELA_GOLD}
        WHERE percentual IS NULL
        ORDER BY 1, 3, 2"""
):
    print(f"      {r[0]:<28} {r[2]}  {r[1]:<26} contagem={r[3]}")

# 3 - Top-N escolhido e volume agrupado em 'Outros'

print(f"\n3️⃣  Top-N e agrupamentos (do JSON de metadados do job)\n")
for d in metadata["dimensoes"]:
    print(f"   {d['coluna']}: cardinalidade={d['cardinalidade']} | top_n_aplicado={d['top_n_aplicado']}")
    if d["top_n_aplicado"]:
        for i, v in enumerate(d["top_n_valores"], 1):
            print(f"      {i:>2}. {v}")
    for chave, qtd in sorted(d.get("agrupados", {}).items()):
        rotulo, ano = chave.split("|")
        print(f"      -> {rotulo} em {ano}: {qtd}")

print(f"\n   Percentual de 'Outros' por ano (dimensões com top-N):")
for r in run_athena_query(
    f"""SELECT dimensao, ano_pesquisa, contagem, ROUND(100 * percentual, 1)
        FROM {DATABASE}.{TABELA_GOLD}
        WHERE valor = 'Outros' ORDER BY 1, 2"""
):
    print(f"      {r[0]:<28} {r[1]}  contagem={r[2]:<6} {r[3]}%")

# 4 - Contagem de linhas da Gold

print(f"\n4️⃣  Linhas da Gold\n")
qtd_linhas = int(run_athena_query(f"SELECT COUNT(*) FROM {DATABASE}.{TABELA_GOLD}")[0][0])
print(f"   Total: {qtd_linhas} linhas (job reportou {metadata['qtd_linhas']})")
for r in run_athena_query(f"SELECT dimensao, ano_pesquisa, COUNT(*) FROM {DATABASE}.{TABELA_GOLD} GROUP BY 1, 2 ORDER BY 1, 2"):
    print(f"      {r[0]:<28} {r[1]}  {r[2]} linhas")
if qtd_linhas != metadata["qtd_linhas"]:
    falhas.append(f"linhas: athena={qtd_linhas} job={metadata['qtd_linhas']}")
if qtd_linhas > 1000:
    falhas.append(f"linhas: {qtd_linhas} (esperado na casa das centenas)")

# 5 - cargo_atual_agrupado: drift de rótulo resolvido

print(f"\n5️⃣  cargo_atual_agrupado - rótulos regulares por ano (esperado: todos presentes nos 3 anos)\n")
anos = sorted({ano for (_, ano) in silver_soma})
# um rótulo só precisa estar nos 3 anos da gold se está nos 3 anos da silver - opções que só existiram
# em 2023 (DBA, Economista...) não são drift, só aviso
presenca_silver = {}
for r in run_athena_query(
    f"""SELECT cargo_atual_agrupado, ano_pesquisa, COUNT(*) FROM {DATABASE}.{TABELA_SILVER}
        WHERE cargo_atual_agrupado IS NOT NULL GROUP BY 1, 2"""
):
    presenca_silver.setdefault(r[0], {})[int(r[1])] = int(r[2])
presenca = {}
for r in run_athena_query(
    f"""SELECT valor, ano_pesquisa, contagem FROM {DATABASE}.{TABELA_GOLD}
        WHERE dimensao = 'cargo_atual_agrupado' AND valor NOT IN ('Outros', 'Não informado')"""
):
    presenca.setdefault(r[0], {})[int(r[1])] = int(r[2])
faltantes = []
for valor in sorted(presenca, key=lambda v: -sum(presenca[v].values())):
    qtds = [presenca[valor].get(a, 0) for a in anos]
    anos_silver = sorted(presenca_silver.get(valor, {}))
    existe_nos_3 = len(anos_silver) == len(anos)
    ok = all(q > 0 for q in qtds) if existe_nos_3 else True
    if not ok:
        faltantes.append(valor)
    marca = "✅" if existe_nos_3 else "ℹ️"
    extra = "" if existe_nos_3 else f"   (só existe em {anos_silver} na Silver - opção de edição única, não é drift)"
    print(f"   {marca} {valor:<62} " + " ".join(f"{q:>5}" for q in qtds) + extra)
if faltantes:
    falhas.append(f"cargo_atual_agrupado com rótulos ausentes em algum ano: {faltantes}")
drift_antigo = [v for v in presenca if "Data Architect" in v]
if drift_antigo:
    falhas.append(f"variantes antigas de Engenheiro/Arquiteto de Dados ainda presentes: {drift_antigo}")
print(f"   Variantes antigas de Eng/Arq de Dados na Gold: {drift_antigo or 'nenhuma'} ✅" if not drift_antigo else f"   ❌ {drift_antigo}")

# 6 - setor sem top-N: todas as categorias, sem 'Outros'

print(f"\n6️⃣  setor - categorias por ano (esperado: 21 regulares, sem 'Outros', SUM(percentual)=1)\n")
cardinalidade_setor = int(run_athena_query(f"SELECT COUNT(DISTINCT setor) FROM {DATABASE}.{TABELA_SILVER}")[0][0])
for r in run_athena_query(
    f"""SELECT ano_pesquisa,
               SUM(CASE WHEN valor NOT IN ('Outros', 'Não informado') THEN 1 ELSE 0 END) AS regulares,
               SUM(CASE WHEN valor = 'Outros' THEN 1 ELSE 0 END) AS outros,
               ROUND(SUM(percentual), 8)
        FROM {DATABASE}.{TABELA_GOLD} WHERE dimensao = 'setor' GROUP BY 1 ORDER BY 1"""
):
    ano, regulares, outros, soma = int(r[0]), int(r[1]), int(r[2]), float(r[3])
    ok = regulares == cardinalidade_setor and outros == 0 and abs(soma - 1.0) < TOLERANCIA_PERCENTUAL
    if not ok:
        falhas.append(f"setor/{ano}: regulares={regulares} (esperado {cardinalidade_setor}) outros={outros} soma={soma}")
    print(f"   {ano}: {regulares} categorias regulares (Silver: {cardinalidade_setor}) | linhas 'Outros'={outros} | SUM(percentual)={soma:.8f}  {'✅' if ok else '❌'}")

# Resumo

print(f"\n{'='*60}")
if falhas:
    print(f"❌ Validação da Gold com {len(falhas)} falha(s):")
    for f in falhas:
        print(f"   - {f}")
    print(f"{'='*60}\n")
    sys.exit(1)
print("✅ Validação da camada Gold concluída!")
print(f"{'='*60}\n")
