# Setup inicial: cria o bucket do projeto, a estrutura de pastas e sobe os 3 csv da pesquisa
# (adaptado da aula de camada bronze)

from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import sys

sys.stdout.reconfigure(encoding="utf-8")

# --- configuração

AWS_REGION = "us-east-1"

# os csv ficam em ../database (não em dados/ como no enunciado)
LOCAL_FOLDER = (Path(__file__).resolve().parents[1] / "database").resolve()

# Mapping de arquivos -> prefixos no S3
S3_PREFIXES = {
    "State_of_data_BR_2023_Kaggle - df_survey_2023.csv": "data-input/bronze/pesquisa-2023/",
    "Final Dataset - State of Data 2024 - Kaggle - df_survey_2024.csv": "data-input/bronze/pesquisa-2024/",
    "Final Dataset - State of Data 2025-2026 - Kaggle.csv": "data-input/bronze/pesquisa-2025/",
}

# Pastas que serão criadas no bucket
FOLDERS = [
    "data-input/bronze/pesquisa-2023/",
    "data-input/bronze/pesquisa-2024/",
    "data-input/bronze/pesquisa-2025/",
    "data-output/bronze/pesquisa-2023/",
    "data-output/bronze/pesquisa-2024/",
    "data-output/bronze/pesquisa-2025/",
    "data-output/silver/state-of-data/",
    "data-output/gold/state-of-data/",
    "athena-logs/",
]


print("📋 Validando credenciais AWS...")
try:
    identity = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()
    ACCOUNT_ID = identity["Account"]
    print(f"   ✅ Autenticado como: {ACCOUNT_ID}")
except Exception as e:
    print(f"   ❌ Erro de autenticação: {str(e)}")
    sys.exit(1)

# bucket = <account_id>-lab, convenção da disciplina
BUCKET_NAME = f"{ACCOUNT_ID}-lab"

s3 = boto3.client("s3", region_name=AWS_REGION)

print("✅ Configuração carregada com sucesso!")
print(f"   Bucket: {BUCKET_NAME}")
print(f"   Região: {AWS_REGION}")
print(f"   Arquivos locais: {LOCAL_FOLDER}")

# --- bucket e pastas

print(f"\n🪣 Criando bucket: s3://{BUCKET_NAME}")
try:
    if AWS_REGION == "us-east-1":
        s3.create_bucket(Bucket=BUCKET_NAME)
    else:
        s3.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
        )
    print(f"   ✅ Bucket criado com sucesso")
except ClientError as e:
    if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
        print(f"   ℹ️  Bucket já existe e é seu")
    elif e.response["Error"]["Code"] == "BucketAlreadyExists":
        print(f"   ❌ Erro: Bucket já existe (propriedade de outro usuário)")
        sys.exit(1)
    else:
        print(f"   ❌ Erro ao criar bucket: {str(e)}")
        sys.exit(1)

print("\n📁 Criando estrutura de pastas...")
failed_folders = []

for folder in FOLDERS:
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=folder)
        print(f"   ✅ {folder}")
    except ClientError as e:
        print(f"   ❌ {folder} - Erro: {str(e)}")
        failed_folders.append(folder)

if failed_folders:
    print(f"\n⚠️  {len(failed_folders)} pasta(s) falharam ao criar")
else:
    print(f"\n✅ Todas as {len(FOLDERS)} pastas criadas com sucesso!")

# --- upload dos csv

print("\n📤 Iniciando upload de arquivos para S3\n")

if not LOCAL_FOLDER.exists():
    print(f"❌ Erro: Pasta não encontrada: {LOCAL_FOLDER}")
    sys.exit(1)

print(f"📁 Procurando arquivos CSV em: {LOCAL_FOLDER}")

csv_files = sorted(list(LOCAL_FOLDER.glob("*.csv")))

if not csv_files:
    print(f"   ⚠️  Nenhum arquivo CSV encontrado")
    sys.exit(1)

print(f"   ✅ Encontrados {len(csv_files)} arquivo(s)\n")

uploaded_files = []
failed_files = []

for file_path in csv_files:
    try:
        if not file_path.is_file():
            print(f"   ⚠️  {file_path.name} - não é um arquivo válido")
            continue

        if file_path.stat().st_size == 0:
            print(f"   ⚠️  {file_path.name} - arquivo vazio, pulando")
            continue

        # só sobe o que está em S3_PREFIXES
        prefix = S3_PREFIXES.get(file_path.name)
        if prefix is None:
            print(f"   ⚠️  {file_path.name} - não mapeado em S3_PREFIXES, pulando")
            continue
        s3_key = f"{prefix}{file_path.name}"

        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        print(f"   ⏳ {file_path.name} ({file_size_mb:.2f} MB)...", end=" ")

        s3.upload_file(str(file_path), BUCKET_NAME, s3_key)

        print(f"✅ → s3://{BUCKET_NAME}/{s3_key}")
        uploaded_files.append((file_path.name, s3_key))

    except FileNotFoundError:
        print(f"   ❌ {file_path.name} - arquivo não encontrado")
        failed_files.append(file_path.name)
    except ClientError as e:
        print(f"   ❌ {file_path.name} - Erro AWS: {str(e)}")
        failed_files.append(file_path.name)
    except Exception as e:
        print(f"   ❌ {file_path.name} - Erro: {str(e)}")
        failed_files.append(file_path.name)

print(f"\n{'='*60}")
print(f"📊 RESUMO DO UPLOAD")
print(f"{'='*60}")
print(f"   ✅ Sucesso: {len(uploaded_files)}/{len(S3_PREFIXES)} arquivo(s)")

if failed_files:
    print(f"   ❌ Falhas:  {len(failed_files)}")
    for file_name in failed_files:
        print(f"      - {file_name}")

print(f"{'='*60}\n")

# --- conferência

print(f"📊 Estrutura do bucket s3://{BUCKET_NAME}:")
try:
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_NAME)

    objetos = []
    for page in pages:
        objetos.extend(page.get("Contents", []))

    if objetos:
        for obj in sorted(objetos, key=lambda x: x["Key"]):
            tamanho_mb = obj["Size"] / (1024 * 1024)
            print(f"   📄 {obj['Key']:<75} ({tamanho_mb:.2f} MB)")
    else:
        print("   (vazio)")
except ClientError as e:
    print(f"   ❌ Erro ao listar: {str(e)}")

if failed_files:
    sys.exit(1)
