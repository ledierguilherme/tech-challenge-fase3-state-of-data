# Relatório consolidado das tabelas gold: baixa os json de metadados que cada job gravou, roda as
# validações no athena e escreve metadata/relatorio_gold.md (linhas, amostra baixa, drift, referências).
#
#   python relatorio_gold.py                     -> tudo + relatório
#   python relatorio_gold.py --tabela X --check  -> só uma tabela, exit 1 se tiver problema real
#                                                   (uso isso no lote pra parar no primeiro problema)

from pathlib import Path
import argparse
import boto3
from botocore.exceptions import ClientError
import json
import sys
import time
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

AWS_REGION = "us-east-1"
DATABASE = "workspace"
TABELA_SILVER = "tb_state_of_data_silver"
METADATA_DIR = Path(__file__).resolve().parent / "metadata"
RELATORIO = METADATA_DIR / "relatorio_gold.md"
TOL = 1e-6

TABELAS = {
    "tb_gold_perfil_mercado":            "data-output/gold/_metadata/tb_gold_perfil_mercado.json",
    "tb_gold_salario_por_perfil":        "data-output/gold/_metadata/tb_gold_salario_por_perfil.json",
    "tb_gold_diversidade":               "data-output/gold/_metadata/tb_gold_diversidade.json",
    "tb_gold_adocao_tecnologia":         "data-output/gold/_metadata/tb_gold_adocao_tecnologia.json",
    "tb_gold_adocao_ia_uso":             "data-output/gold/_metadata/tb_gold_adocao_ia_uso.json",
    "tb_gold_adocao_ia_pessoal":         "data-output/gold/_metadata/tb_gold_adocao_ia_pessoal.json",
    "tb_gold_barreiras_ia":              "data-output/gold/_metadata/tb_gold_barreiras_ia.json",
    "tb_gold_regiao_senioridade_modelo": "data-output/gold/_metadata/tb_gold_regiao_senioridade_modelo.json",
}

# números que conferi no athena enquanto montava a silver - se algum não bater, o job leu a coluna errada
REF_TEC = {("tec_usa_python", 2023): 2825, ("tec_usa_python", 2024): 2935, ("tec_pref_python", 2025): 1929,
           ("tec_usa_sql", 2023): 3156, ("tec_usa_sql", 2024): 3146}
REF_TEC_TOTAL = {("tec_usa_python", 2023): 3772, ("tec_usa_python", 2024): 3589, ("tec_pref_python", 2025): 2096}
REF_IA_USO = {("ia_emp_uso_independente", 2023): 303, ("ia_emp_uso_independente", 2024): 395, ("ia_emp_uso_independente", 2025): 312,
              ("ia_ind_uso_independente", 2023): 1723, ("ia_ind_uso_independente", 2024): 1811, ("ia_ind_uso_independente", 2025): 1017,
              ("ia_ind_copilots_dev", 2023): 578, ("ia_ind_copilots_dev", 2024): 948, ("ia_ind_copilots_dev", 2025): 685}
REF_IA_PESSOAL_TOTAL = {2023: 3772, 2024: 3619, 2025: 2106}
REF_BARREIRAS = {("ia_emp_motivo_casos_uso", 2023): 303, ("ia_emp_motivo_casos_uso", 2024): 334, ("ia_emp_motivo_casos_uso", 2025): 188}
REF_BARREIRAS_TOTAL = {2023: 823, 2024: 974, 2025: 587}
REF_RESPONDENTES = {2023: 5293, 2024: 5217, 2025: 3495}

parser = argparse.ArgumentParser()
parser.add_argument("--tabela", help="validar só esta tabela")
parser.add_argument("--check", action="store_true", help="exit 1 se houver problema real (drift não mapeado ou total divergente)")
cli = parser.parse_args()

account_id = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()["Account"]
BUCKET_NAME = f"{account_id}-lab"
ATHENA_OUTPUT = f"s3://{BUCKET_NAME}/athena-logs/"
s3 = boto3.client("s3", region_name=AWS_REGION)
athena = boto3.client("athena", region_name=AWS_REGION)


def q(sql: str, max_wait_time: int = 120):
    ex = athena.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": DATABASE},
                                      ResultConfiguration={"OutputLocation": ATHENA_OUTPUT})["QueryExecutionId"]
    t = 0
    while t < max_wait_time:
        st = athena.get_query_execution(QueryExecutionId=ex)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if st["State"] != "SUCCEEDED":
                raise RuntimeError(f"Query {st['State']}: {st.get('StateChangeReason')}")
            break
        time.sleep(2); t += 2
    rows = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=ex):
        for r in page["ResultSet"]["Rows"]:
            rows.append([c.get("VarCharValue") for c in r["Data"]])
    return rows[1:]


def check(cond, msg, problemas, avisos=None):
    print(f"   {'✅' if cond else '❌'} {msg}")
    if not cond:
        problemas.append(msg)


# validações por tabela (cada uma vai enchendo a lista de problemas)

def validar_perfil_mercado(m, p):
    n = int(q("SELECT COUNT(*) FROM tb_gold_perfil_mercado")[0][0])
    check(n == m["qtd_linhas"], f"linhas Athena={n} job={m['qtd_linhas']}", p)


def validar_salario_por_perfil(m, p):
    gold = {int(r[0]): int(r[1]) for r in q("SELECT ano_pesquisa, SUM(contagem) FROM tb_gold_salario_por_perfil WHERE regiao_onde_mora <> 'Todas' AND nivel_senioridade_agrupada <> 'Todos' GROUP BY 1")}
    for r in q("SELECT ano_pesquisa, SUM(contagem) FROM tb_gold_salario_por_perfil WHERE regiao_onde_mora = 'Todas' AND nivel_senioridade_agrupada = 'Todos' GROUP BY 1 ORDER BY 1"):
        check(int(r[1]) == gold.get(int(r[0])), f"{r[0]}: agregado Todas/Todos={r[1]} x cruzamento completo={gold.get(int(r[0]))}", p)
    silver = {int(r[0]): int(r[1]) for r in q(f"SELECT ano_pesquisa, COUNT(faixa_salarial_ponto_medio) FROM {TABELA_SILVER} GROUP BY 1")}
    for ano in sorted(silver):
        check(gold.get(ano) == silver[ano], f"{ano}: SUM(contagem)={gold.get(ano)} x COUNT(salário) Silver={silver[ano]}", p)


def validar_diversidade(m, p):
    for r in q("SELECT recorte, ano_pesquisa, ROUND(SUM(percentual), 8) FROM tb_gold_diversidade WHERE nivel_senioridade_agrupada = 'Todos' AND percentual IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2"):
        check(abs(float(r[2]) - 1.0) < TOL, f"'Todos' {r[0]}/{r[1]}: SUM(percentual)={r[2]}", p)
    for r in q("SELECT ano_pesquisa, SUM(contagem) FROM tb_gold_diversidade WHERE recorte='genero' AND nivel_senioridade_agrupada='Todos' GROUP BY 1 ORDER BY 1"):
        check(int(r[1]) == REF_RESPONDENTES[int(r[0])], f"genero/Todos {r[0]}: SUM(contagem)={r[1]} x respondentes={REF_RESPONDENTES[int(r[0])]}", p)


def validar_adocao_tecnologia(m, p):
    obt = {(r[0], int(r[1])): (int(r[2]), int(r[3])) for r in q("SELECT fonte, ano_pesquisa, contagem_true, total_respondentes FROM tb_gold_adocao_tecnologia WHERE fonte IN ('tec_usa_python','tec_usa_sql','tec_pref_python')")}
    for k, esperado in REF_TEC.items():
        check(obt.get(k, (None,))[0] == esperado, f"{k[0]} {k[1]}: contagem_true={obt.get(k, (None,))[0]} esperado={esperado}", p)
    for k, esperado in REF_TEC_TOTAL.items():
        check(obt.get(k, (None, None))[1] == esperado, f"{k[0]} {k[1]}: total_respondentes={obt.get(k, (None, None))[1]} esperado={esperado}", p)
    n2025_usa = int(q("SELECT COUNT(*) FROM tb_gold_adocao_tecnologia WHERE tipo_metrica='uso_dia_a_dia' AND ano_pesquisa=2025")[0][0])
    check(n2025_usa == 0, f"uso_dia_a_dia em 2025: {n2025_usa} linhas (esperado 0)", p)


def validar_adocao_ia_uso(m, p):
    obt = {(r[0], int(r[1])): int(r[2]) for r in q("SELECT fonte, ano_pesquisa, contagem_true FROM tb_gold_adocao_ia_uso WHERE fonte IN ('ia_emp_uso_independente','ia_ind_uso_independente','ia_ind_copilots_dev')")}
    for k, esperado in REF_IA_USO.items():
        check(obt.get(k) == esperado, f"{k[0]} {k[1]}: contagem_true={obt.get(k)} esperado={esperado}", p)


def validar_adocao_ia_pessoal(m, p):
    tot = {int(r[0]): int(r[1]) for r in q("SELECT ano_pesquisa, SUM(contagem) FROM tb_gold_adocao_ia_pessoal WHERE nivel_uso <> 'Não informado' GROUP BY 1")}
    for ano, esperado in REF_IA_PESSOAL_TOTAL.items():
        check(tot.get(ano) == esperado, f"{ano}: respondentes com nivel_uso={tot.get(ano)} esperado={esperado}", p)
    for r in q("SELECT nivel_senioridade_agrupada, ano_pesquisa, ROUND(SUM(percentual), 8) FROM tb_gold_adocao_ia_pessoal WHERE percentual IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2"):
        check(abs(float(r[2]) - 1.0) < TOL, f"{r[0]}/{r[1]}: SUM(percentual)={r[2]}", p)


def validar_barreiras_ia(m, p):
    obt = {int(r[0]): (int(r[1]), int(r[2])) for r in q("SELECT ano_pesquisa, contagem_true, total_respondentes FROM tb_gold_barreiras_ia WHERE fonte='ia_emp_motivo_casos_uso'")}
    for (fonte, ano), esperado in REF_BARREIRAS.items():
        check(obt.get(ano, (None,))[0] == esperado, f"{fonte} {ano}: contagem_true={obt.get(ano, (None,))[0]} esperado={esperado}", p)
    for ano, esperado in REF_BARREIRAS_TOTAL.items():
        check(obt.get(ano, (None, None))[1] == esperado, f"{ano}: total_respondentes={obt.get(ano, (None, None))[1]} esperado={esperado}", p)


def validar_regiao_senioridade_modelo(m, p):
    tot = {int(r[0]): int(r[1]) for r in q("SELECT ano_pesquisa, SUM(contagem) FROM tb_gold_regiao_senioridade_modelo GROUP BY 1")}
    for ano, esperado in REF_RESPONDENTES.items():
        check(tot.get(ano) == esperado, f"{ano}: SUM(contagem)={tot.get(ano)} esperado={esperado}", p)
    n = int(q("SELECT COUNT(*) FROM tb_gold_regiao_senioridade_modelo WHERE ano_pesquisa=2023 AND modelo_trabalho_atual <> 'Não perguntado em 2023'")[0][0])
    check(n == 0, f"2023 sem 'Não perguntado em 2023': {n} linhas (esperado 0)", p)


VALIDADORES = {
    "tb_gold_perfil_mercado": validar_perfil_mercado,
    "tb_gold_salario_por_perfil": validar_salario_por_perfil,
    "tb_gold_diversidade": validar_diversidade,
    "tb_gold_adocao_tecnologia": validar_adocao_tecnologia,
    "tb_gold_adocao_ia_uso": validar_adocao_ia_uso,
    "tb_gold_adocao_ia_pessoal": validar_adocao_ia_pessoal,
    "tb_gold_barreiras_ia": validar_barreiras_ia,
    "tb_gold_regiao_senioridade_modelo": validar_regiao_senioridade_modelo,
}

# execução

alvo = [cli.tabela] if cli.tabela else list(TABELAS)
METADATA_DIR.mkdir(parents=True, exist_ok=True)
resultados = {}
problemas_gerais = []

for tabela in alvo:
    print(f"\n{'='*70}\n{tabela}\n{'='*70}")
    local = METADATA_DIR / f"{tabela}.json"
    try:
        s3.download_file(BUCKET_NAME, TABELAS[tabela], str(local))
    except ClientError as e:
        print(f"   ❌ metadados não encontrados ({e}); o job rodou?")
        problemas_gerais.append(f"{tabela}: metadados ausentes")
        continue
    m = json.loads(local.read_text(encoding="utf-8"))
    n_athena = int(q(f"SELECT COUNT(*) FROM {tabela}")[0][0])
    problemas = []
    check(n_athena == m["qtd_linhas"], f"linhas: Athena={n_athena} job={m['qtd_linhas']}", problemas)
    VALIDADORES[tabela](m, problemas)
    drift = m.get("drift", [])
    for d in drift:
        print(f"   ⚠️  DRIFT {d}")
    amostra = m.get("amostra_baixa_casos", [])
    print(f"   ℹ️  amostra_baixa=true: {len(amostra)} célula(s)")
    if drift:
        problemas.append(f"drift não mapeado: {len(drift)} rótulo(s)")
    resultados[tabela] = {"linhas": n_athena, "problemas": problemas, "drift": drift, "amostra_baixa": amostra, "meta": m}
    problemas_gerais += [f"{tabela}: {x}" for x in problemas]

# relatório em markdown (só quando roda tudo)

if not cli.tabela:
    linhas = ["# Relatório consolidado — camada Gold\n", f"Gerado em {datetime.now().isoformat(timespec='seconds')} · banco `{DATABASE}` · bucket `{BUCKET_NAME}`\n"]
    linhas.append("\n## 1. Linhas por tabela\n\n| tabela | linhas | pergunta respondida |\n|---|---:|---|")
    for t, r in resultados.items():
        linhas.append(f"| `{t}` | {r['linhas']} | {r['meta'].get('pergunta_respondida', '')} |")
    linhas.append("\n## 2. Células com amostra_baixa = true (contagem < 30)\n")
    for t, r in resultados.items():
        casos = r["amostra_baixa"]
        linhas.append(f"\n### `{t}` — {len(casos)} célula(s)\n")
        if casos:
            chaves = [k for k in casos[0] if k != "contagem"]
            linhas.append("| " + " | ".join(chaves) + " | contagem |\n|" + "---|" * (len(chaves) + 1))
            for c in casos:
                linhas.append("| " + " | ".join(str(c[k]) for k in chaves) + f" | {c['contagem']} |")
    linhas.append("\n## 3. Drift de rótulo detectado (contagem 0 num ano em que o rótulo existe nos outros)\n")
    algum = False
    for t, r in resultados.items():
        for d in r["drift"]:
            algum = True
            linhas.append(f"- `{t}` · {d}")
    if not algum:
        linhas.append("Nenhum drift não mapeado.")
    linhas.append("\n## 4. Validações e referências numéricas\n")
    for t, r in resultados.items():
        status = "✅ OK" if not r["problemas"] else "❌ " + "; ".join(r["problemas"])
        linhas.append(f"- `{t}`: {status}")
    linhas.append("\n## 5. Metadados\n")
    for t in resultados:
        linhas.append(f"- `metadata/{t}.json`")
    RELATORIO.write_text("\n".join(linhas) + "\n", encoding="utf-8")
    print(f"\n📄 Relatório: {RELATORIO}")

print(f"\n{'='*70}")
if problemas_gerais:
    print(f"❌ {len(problemas_gerais)} problema(s):")
    for x in problemas_gerais:
        print(f"   - {x}")
    sys.exit(1 if cli.check else 0)
print("✅ Sem problemas reais nas tabelas validadas")
