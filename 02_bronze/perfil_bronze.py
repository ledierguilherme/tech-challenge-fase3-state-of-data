# DISTINCT + COUNT de cada campo por ano, pra comparar se os rótulos mudaram entre as edições
# uso: python perfil_bronze.py [campo ...]   (sem argumento roda todos)

import boto3
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")

## Configuração Global - Variáveis Reutilizáveis

AWS_REGION = "us-east-1"
DATABASE = "workspace"
ANOS = ["2023", "2024", "2025"]

# campo -> nome bronze em cada ano (peguei dos json de _metadata)
# atenção que os códigos das perguntas mudam de posição entre 2024 e 2025 (2.r -> 2.q, 3.g -> 3.h...)
CAMPOS = {
    "genero": {
        "2023": "P1_b_Genero",
        "2024": "1_b_genero",
        "2025": "1_b_genero",
    },
    "cor_raca_etnia": {
        "2023": "P1_c_Cor_raca_etnia",
        "2024": "1_c_cor_raca_etnia",
        "2025": "1_c_cor_raca_etnia",
    },
    "uf_onde_mora": {
        "2023": "P1_i_1_uf_onde_mora",
        "2024": "1_i_1_uf_onde_mora",
        "2025": "1_i_1_uf_onde_mora",
    },
    "regiao_onde_mora": {
        "2023": "P1_i_2_Regiao_onde_mora",
        "2024": "1_i_2_regiao_onde_mora",
        "2025": "1_i_2_regiao_onde_mora",
    },
    "nivel_ensino": {
        "2023": "P1_l_Nivel_de_Ensino",
        "2024": "1_l_nivel_de_ensino",
        "2025": "1_l_nivel_de_ensino",
    },
    "area_formacao": {
        "2023": "P1_m_Área_de_Formação",
        "2024": "1_m_área_de_formação",
        "2025": "1_m_área_de_formação",
    },
    "cargo_atual": {
        "2023": "P2_f_Cargo_Atual",
        "2024": "2_f_cargo_atual",
        "2025": "2_f_cargo_atual",
    },
    "nivel_senioridade": {
        "2023": "P2_g_Nivel",
        "2024": "2_g_nivel",
        "2025": "2_g_nivel",
    },
    "faixa_salarial": {
        "2023": "P2_h_Faixa_salarial",
        "2024": "2_h_faixa_salarial",
        "2025": "2_h_faixa_salarial",
    },
    "setor": {
        "2023": "P2_b_Setor",
        "2024": "2_b_setor",
        "2025": "2_b_setor",
    },
    "numero_funcionarios": {
        "2023": "P2_c_Numero_de_Funcionarios",
        "2024": "2_c_numero_de_funcionarios",
        "2025": "2_c_numero_de_funcionarios",
    },
    "tempo_experiencia_dados": {
        "2023": "P2_i_Quanto_tempo_de_experiência_na_área_de_dados_você_tem",
        "2024": "2_i_tempo_de_experiencia_em_dados",
        "2025": "2_i_tempo_de_experiencia_em_dados",
    },
    "modelo_trabalho_atual": {
        "2023": None,  # não tem em 2023
        "2024": "2_r_modelo_de_trabalho_atual",
        "2025": "2_q_modelo_de_trabalho_atual",
    },
    "ai_generativa_prioridade": {
        "2023": "P3_e_AI_Generativa_é_uma_prioridade_em_sua_empresa",
        "2024": "3_e_ai_generativa_e_llm_é_uma_prioridade",
        "2025": "3_e_ai_generativa_e_llm_é_uma_prioridade",
    },
}

print("📋 Validando credenciais AWS...")
try:
    account_id = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()["Account"]
    print(f"   ✅ Autenticado como: {account_id}")
except Exception as e:
    print(f"   ❌ Erro de autenticação: {str(e)}")
    sys.exit(1)

BUCKET_NAME = f"{account_id}-lab"
ATHENA_OUTPUT = f"s3://{BUCKET_NAME}/athena-logs/"

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
    return rows[1:]  # sem o cabeçalho


def perfil_campo(campo: str, colunas_por_ano: dict):
    print(f"\n{'#'*90}")
    print(f"# {campo}")
    print(f"{'#'*90}")

    for ano in ANOS:
        coluna = colunas_por_ano.get(ano)
        if not coluna:
            print(f"\n--- {ano}: campo não existe nesta edição")
            continue

        tabela = f"tb_pesquisa_{ano}_bronze"
        query = (
            f'SELECT DISTINCT "{coluna}" AS valor, COUNT(*) AS qtd '
            f'FROM "{DATABASE}"."{tabela}" '
            f'GROUP BY "{coluna}" ORDER BY qtd DESC'
        )
        try:
            rows = run_athena_query(query)
        except Exception as e:
            print(f"\n--- {ano} ({coluna}) ❌ erro: {str(e)}")
            continue

        print(f"\n--- {ano} ({coluna}) — {len(rows)} valores distintos")
        for valor, qtd in rows:
            print(f"  {qtd:>6}  {valor if valor is not None else '<NULL>'}")


## Execução

campos_solicitados = sys.argv[1:] or list(CAMPOS.keys())
desconhecidos = [c for c in campos_solicitados if c not in CAMPOS]
if desconhecidos:
    print(f"❌ Campo(s) desconhecido(s): {desconhecidos}")
    print(f"   Disponíveis: {', '.join(CAMPOS.keys())}")
    sys.exit(1)

print(f"\n🔎 Perfilando {len(campos_solicitados)} campo(s) via Athena (output: {ATHENA_OUTPUT})")

for campo in campos_solicitados:
    perfil_campo(campo, CAMPOS[campo])

print(f"\n{'='*60}")
print("✅ Perfil da camada Bronze concluído!")
print(f"{'='*60}\n")
