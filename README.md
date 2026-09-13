# Pipeline de Dados — State of Data Brasil (2023 · 2024 · 2025)

Tech Challenge Fase 3 (Pós Tech FIAP). Arquitetura medalhão na AWS (S3 + Glue + Athena),
seguindo o padrão do material da aula *Pipeline de Dados - Camada Bronze*.

## Convenções

| Item | Valor |
|---|---|
| Região | `us-east-1` |
| Bucket do projeto | `<account_id>-lab` (descoberto via `sts get-caller-identity`) |
| Bucket de assets do Glue | `aws-glue-assets-<account_id>-us-east-1/scripts/` |
| Role IAM | `LabRole` |
| Banco no Glue Catalog | `workspace` |
| Registro de tabelas | `CREATE TABLE ... USING parquet LOCATION` + `MSCK REPAIR TABLE` (sem Crawler) |
| Output do Athena | `s3://<bucket>/athena-logs/` |

## Estrutura

```

├── 01_infra/                          # uma vez por conta/sessão do Lab
│   ├── aws_credentials.md             # credenciais temporárias (ignorado no git)
│   └── setup_s3.py                    # cria bucket + pastas e sobe os CSVs de ./database
├── 02_bronze/                         # camada Bronze
│   ├── glue-state-of-data-bronze.py   # script PySpark que RODA NO GLUE (job glue-state-of-data-bronze)
│   ├── deploy_glue_job.py             # sobe o script, cria/atualiza o job, executa e monitora
│   ├── validar_bronze.py              # lista objetos no S3 + SHOW TABLES + contagem via Athena
│   └── perfil_bronze.py               # DISTINCT/COUNT por campo e ano (comparar rótulos entre edições)
├── 03_silver/                         # camada Silver
│   ├── Camada_Silver_-_State_of_Data.ipynb   # Glue Notebook interativo (union das 3 edições + harmonização + extensão)
│   └── metadata/tb_state_of_data_silver_colunas.json   # cópia local do mapeamento Bronze->Silver resolvido (rastreabilidade)
└── 04_gold/                           # camada Gold (jobs batch de agregação, reexecutáveis)
    ├── gold_perfil_mercado.py         # 7 scripts PySpark que RODAM NO GLUE (1 por tabela; job glue-state-of-data-<script>)
    ├── gold_salario_por_perfil.py
    ├── gold_diversidade.py
    ├── gold_adocao_tecnologia.py
    ├── gold_adocao_ia.py              # gera 2 tabelas (uso + pessoal)
    ├── gold_barreiras_ia.py
    ├── gold_regiao_senioridade_modelo.py
    ├── deploy_glue_job.py             # --script gold_<tabela>.py: sobe o script, cria/atualiza o job, executa e monitora
    ├── validar_gold.py                # validações detalhadas da tb_gold_perfil_mercado
    ├── relatorio_gold.py              # validações Athena de todas as tabelas + relatório consolidado (--tabela X --check)
    └── metadata/                      # 1 JSON por tabela (schema, regras, amostra_baixa, drift, referências) + relatorio_gold.md
```

Convenção de nomes dentro de cada camada:

- `glue-<camada>.py` / `gold_<tabela>.py` — código PySpark que executa dentro do Glue (nunca rode localmente).
- `deploy_*.py` / `validar_*.py` / `perfil_*.py` — utilitários boto3 executados na máquina local.

## Ordem de execução

```bash
pip install boto3 pandas
aws configure            # credenciais do AWS Academy Lab (ver 01_infra/aws_credentials.md)

python 01_infra/setup_s3.py
python 02_bronze/deploy_glue_job.py
python 02_bronze/validar_bronze.py
python 02_bronze/perfil_bronze.py genero faixa_salarial   # ou sem argumentos = todos

# Silver: abrir o notebook no Glue Studio (Notebook > Upload) OU executar daqui com o kernel glue_pyspark:
pip install aws-glue-sessions jupyter nbconvert
python -m jupyter_client.kernelspecapp install <site-packages>/aws_glue_interactive_sessions_kernel/glue_pyspark --user --name glue_pyspark
python -m jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.kernel_name=glue_pyspark \n    --ExecutePreprocessor.timeout=1500 03_silver/Camada_Silver_-_State_of_Data.ipynb

# Gold (1 job por tabela; a ordem importa só para salario_por_perfil, que reaproveita o top-15 de perfil_mercado)
python 04_gold/deploy_glue_job.py --script gold_perfil_mercado.py
python 04_gold/deploy_glue_job.py --script gold_salario_por_perfil.py
python 04_gold/deploy_glue_job.py --script gold_diversidade.py
python 04_gold/deploy_glue_job.py --script gold_adocao_tecnologia.py
python 04_gold/deploy_glue_job.py --script gold_adocao_ia.py
python 04_gold/deploy_glue_job.py --script gold_barreiras_ia.py
python 04_gold/deploy_glue_job.py --script gold_regiao_senioridade_modelo.py
python 04_gold/relatorio_gold.py          # gera metadata/relatorio_gold.md
```

## Layout no S3

```
s3://<account_id>-lab/
├── data-input/bronze/pesquisa-{2023,2024,2025}/   # CSV original
├── data-output/bronze/
│   ├── pesquisa-{2023,2024,2025}/ano_pesquisa=<ano>/   # Parquet (tb_pesquisa_<ano>_bronze)
│   └── _metadata/pesquisa-<ano>_colunas.json          # mapeamento nome original -> nome bronze
├── data-output/silver/
│   ├── state-of-data/ano_pesquisa=<ano>/              # Parquet (tb_state_of_data_silver, 14.005 linhas x 79 colunas)
│   └── _metadata/tb_state_of_data_silver_colunas.json # coluna silver -> origem por ano + regra de resolução
├── data-output/gold/
│   ├── state-of-data/<tabela>/ano_pesquisa=<ano>/     # Parquet de cada tb_gold_* (8 tabelas)
│   └── _metadata/tb_gold_<tabela>.json                # schema, regras, amostra_baixa, drift, referências (gerado pelo job)
└── athena-logs/
```

## Decisões da Bronze

- Tudo lido como `string` (`inferSchema=false`), `escape='"'` (os CSVs escapam aspas duplicando-as).
- Nomes de coluna recebem **apenas sanitização técnica** (caracteres inválidos → `_`), porque o
  Hive metastore do Glue Catalog rejeita vírgulas em nomes de coluna. Sem lowercase, sem remover
  acentos, sem renomear semanticamente — isso é responsabilidade da Silver, que deve usar o
  `_metadata/pesquisa-<ano>_colunas.json`.
- Partição única por tabela: `ano_pesquisa`.

## Decisões da Silver

- **Union, não join**: as 3 edições são empilhadas com `unionByName(allowMissingColumns=True)`; 15 colunas
  mapeadas + `ano_pesquisa` (partição, `int`).
- Resolução de nomes de origem **case-insensitive** (o Glue Catalog guarda colunas em minúsculas; o Parquet
  preserva o case), então o mapeamento no notebook usa os nomes exatos do `_metadata` da Bronze.
- Harmonização (antes do union): `nivel_senioridade_agrupada` (`Especialista/Staff+` → `Sênior`, só existe em
  2025); 2 typos de `faixa_salarial` corrigidos; `faixa_salarial_ordem` (int) e `faixa_salarial_ponto_medio`
  (double) via tabela de referência de 13 faixas. NULLs das demais colunas são estruturais e ficam como estão.
- `cargo_atual_agrupado` harmoniza a fusão/separação de 'Engenheiro de Dados' e 'Arquiteto de Dados' entre
  edições da pesquisa (unidos em 2023, separados a partir de 2024) no rótulo único
  `Engenheiro de Dados/Arquiteto de Dados`; demais cargos mantidos (incluindo NULL). Mesmo padrão de
  `nivel_senioridade_agrupada`.
- Registro via `saveAsTable` (padrão do notebook Silver da aula), particionado por `ano_pesquisa`.
- **Extensão (tecnologias / IA Generativa)**: 48 colunas resolvidas pelo *sufixo descritivo* do nome da coluna
  Bronze (`<código_pergunta>_<texto>`, regex `^[A-Za-z]?\d+(_[A-Za-z]{1,2})?(_\d+)*_`), normalizado (minúsculas, sem
  acento), restrito à seção (3 = empresa, 4 = indivíduo) e à mesma pergunta de uma coluna-âncora. Sub-itens `'1'/'0'`
  viram boolean; ausentes no ano viram `lit(None)`. `ia_pessoal_nivel_uso` é derivada dos 5 booleanos de
  produtividade pessoal. Prefixos: `tec_usa_*`, `tec_pref_*`, `ia_emp_*`, `ia_emp_motivo_*`, `ia_ind_*`, `ia_pessoal_*`.

> **Nota metodológica (tecnologia).** A partir de 2025, a pesquisa não repete a pergunta "linguagens usadas no
> dia a dia" (substituída por "linguagem preferida", de natureza diferente) — comparação de adoção de tecnologia
> 2023→2025 fica limitada a 2023–2024; 2025 é reportado separadamente como "linguagem preferida".
> Na Silver: `tec_usa_*` e `tec_linguagem_mais_usada` só existem em 2023/2024 (NULL em 2025); `tec_pref_*`
> (booleanos dos sub-itens `4_c_*`) só em 2025; `tec_pref_normalizada` (texto livre normalizado) só em 2023/2024.
> Em 2024 o sub-item "Não utilizo nenhuma linguagem" veio 100% `"0"` da fonte, então `tec_usa_nenhuma` é derivado
> da guarda-chuva `4_d_linguagem_de_programacao_dia_a_dia` (`contains('Não utilizo')`).

## Decisões da Gold — `tb_gold_perfil_mercado`

- Formato **longo (tidy)**: uma linha por `(dimensao, valor, ano_pesquisa)` com `contagem` e `percentual`;
  não é um cross-tab. Dimensões: `cargo_atual_agrupado` (versão harmonizada; top-15 + `Outros`),
  `nivel_senioridade_agrupada`, `setor` (sem corte, 21 categorias), `numero_funcionarios`,
  `modelo_trabalho_atual` (linha explícita `Não perguntado em 2023`). O top-N é configurável por dimensão
  em `build_config` (`top_n = {dimensao: N | None}`).
- `percentual = contagem / soma das categorias regulares` de `(dimensao, ano)`. Linhas `Não informado`
  (NULL na Silver) e `Não perguntado em <ano>` mantêm a contagem mas têm `percentual = NULL` e ficam fora do
  denominador — distingue "não respondeu" de "pergunta inexistente". Com top-N, `Outros` reúne só valores
  não nulos fora do top-N (NULL continua `Não informado`).
- Top-N é calculado no job pelo volume total dos 3 anos e registrado no JSON de metadados (não é hardcoded).
- Registro via `CREATE TABLE ... USING parquet LOCATION` + `MSCK REPAIR TABLE` (padrão da Bronze); 1 arquivo
  Parquet por partição.

## Tabelas Gold e as perguntas que respondem

| tabela | grão | responde |
|---|---|---|
| `tb_gold_perfil_mercado` | (dimensao, valor, ano) | Perfil do mercado: distribuição de cargo, senioridade, setor, porte da empresa e modelo de trabalho por edição |
| `tb_gold_salario_por_perfil` | cargo × senioridade × região × ano | Quanto ganha cada perfil (média e mediana da faixa salarial) e como evoluiu |
| `tb_gold_diversidade` | recorte (gênero / cor-raça) × senioridade (+ 'Todos') × ano | Diversidade por nível de senioridade e salário médio por grupo |
| `tb_gold_adocao_tecnologia` | tecnologia × tipo_metrica × ano | Linguagens usadas no dia a dia (2023–2024) e preferidas (2023–2025) |
| `tb_gold_adocao_ia_uso` | bloco (empresa / indivíduo) × item × ano | Como empresas e profissionais usam IA generativa |
| `tb_gold_adocao_ia_pessoal` | senioridade × nível de uso pessoal × ano | Nível de uso pessoal de IA generativa (não usa / gratuito-copilot / paga / empresa paga) por senioridade |
| `tb_gold_barreiras_ia` | motivo × ano | Barreiras para adoção de IA generativa nas empresas |
| `tb_gold_regiao_senioridade_modelo` | região × senioridade × modelo de trabalho × ano | Modelo de trabalho por região e senioridade, com salário médio |

Regras comuns às Gold: `Não informado` (NULL) e `Não perguntado em <ano>` têm contagem preenchida, métricas NULL e
ficam fora dos denominadores; `amostra_baixa = true` quando a contagem da célula é < 30 (não é filtrada — o
consumidor decide); drift de rótulo (contagem 0 num ano em que o rótulo existe nos outros) é detectado no job e
consolidado em `metadata/relatorio_gold.md`. Em `tb_gold_adocao_tecnologia`, a coluna `fonte` documenta a mudança
de fonte entre edições ('preferida' vem de texto livre normalizado em 2023/2024 e de multi-select em 2025).
