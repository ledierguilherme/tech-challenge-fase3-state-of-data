# Deploy dos jobs gold. É o mesmo deploy da bronze, só que recebe --script porque são 7 jobs
# e não fazia sentido ter 7 cópias. O nome do job sai do nome do arquivo:
#   gold_perfil_mercado.py -> glue-state-of-data-gold-perfil-mercado

from pathlib import Path
import argparse
import boto3
from botocore.exceptions import ClientError
import sys
import time
import traceback
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

# -- Configuração Global - Variáveis Reutilizáveis

AWS_REGION = "us-east-1"
ROLE_NAME = "LabRole"  # Role padrão do AWS Academy Lab

parser = argparse.ArgumentParser(description="Deploy + execução de um Glue Job da camada Gold")
parser.add_argument("--script", default="gold_perfil_mercado.py", help="arquivo .py do job (na pasta 04_gold)")
cli_args = parser.parse_args()

SCRIPT_FILE_NAME = cli_args.script
JOB_NAME = "glue-state-of-data-" + Path(SCRIPT_FILE_NAME).stem.replace("_", "-")

print("📋 Validando credenciais AWS...")
try:
    account_id = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()["Account"]
    print(f"   ✅ Autenticado como: {account_id}")
except Exception as e:
    print(f"   ❌ Erro de autenticação: {str(e)}")
    sys.exit(1)

BUCKET_NAME = f"{account_id}-lab"

# O script do job deve ficar no bucket de assets do Glue, e não no bucket do projeto
glue_assets_bucket = f"aws-glue-assets-{account_id}-{AWS_REGION}"
script_s3_key = f"scripts/{SCRIPT_FILE_NAME}"
script_s3_uri = f"s3://{glue_assets_bucket}/{script_s3_key}"

s3 = boto3.client("s3", region_name=AWS_REGION)
glue_client = boto3.client("glue", region_name=AWS_REGION)
iam_client = boto3.client("iam", region_name=AWS_REGION)
logs_client = boto3.client("logs", region_name=AWS_REGION)

# -- PASSO 4 - Upload do Script Glue Job para S3

print("\n📤 Fazendo upload do script Glue Job para o bucket de assets do Glue...\n")

script_path = str((Path(__file__).resolve().parent / SCRIPT_FILE_NAME).resolve())

print(f"   📍 Procurando script em: {script_path}")

if not Path(script_path).exists():
    print(f"   ❌ Erro: Script não encontrado em {script_path}")
    print(f"   💡 Certifique-se de que o arquivo '{SCRIPT_FILE_NAME}' existe na pasta '04_gold'")
    sys.exit(1)

print(f"   ✅ Script encontrado\n")

# bucket de assets pode não existir na conta do lab
print(f"   📦 Bucket do Glue: s3://{glue_assets_bucket}")
try:
    s3.head_bucket(Bucket=glue_assets_bucket)
    print(f"   ℹ️  Bucket de assets já existe")
except ClientError as e:
    if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
        try:
            s3.create_bucket(Bucket=glue_assets_bucket)
            print(f"   ✅ Bucket de assets criado")
        except ClientError as ce:
            print(f"   ❌ Erro ao criar bucket de assets: {str(ce)}")
            sys.exit(1)
    else:
        print(f"   ❌ Erro ao verificar bucket de assets: {str(e)}")
        sys.exit(1)

try:
    print(f"   📤 Fazendo upload: {script_path}")

    s3.upload_file(script_path, glue_assets_bucket, script_s3_key)

    print(f"   ✅ Script enviado com sucesso!")
    print(f"   📍 Localização: {script_s3_uri}\n")

    response = s3.head_object(Bucket=glue_assets_bucket, Key=script_s3_key)
    print(f"   📊 Detalhes:")
    print(f"      Tamanho: {response['ContentLength'] / 1024:.2f} KB")
    print(f"      Last Modified: {response['LastModified']}\n")

except ClientError as e:
    print(f"❌ Erro ao fazer upload: {str(e)}\n")
    sys.exit(1)
except Exception as e:
    print(f"❌ Erro inesperado: {str(e)}\n")
    sys.exit(1)

print(f"{'='*70}")
print("✅ Script pronto para ser usado em Glue Job!\n")

# -- PASSO 5 - Criar Glue Job via Boto3 (Sem Terraform)

print("🏗️  Criando Glue Job no AWS Glue...\n")

print(f"📋 Configuração:")
print(f"   Job Name: {JOB_NAME}")
print(f"   Script S3: {script_s3_uri}")
print(f"   Bucket do Glue: {glue_assets_bucket}")
print(f"   Bucket do projeto: {BUCKET_NAME}")
print(f"   Role: {ROLE_NAME}\n")

# -- PASSO 5.1 - Obter ARN da LabRole (AWS Academy)

print(f"1️⃣  Obtendo ARN da IAM Role '{ROLE_NAME}'...\n")

try:
    role_response = iam_client.get_role(RoleName=ROLE_NAME)
    role_arn = role_response["Role"]["Arn"]
    print(f"   ✅ Role encontrada!")
    print(f"   📍 ARN: {role_arn}\n")
except Exception as e:
    print(f"   ❌ Erro ao obter Role: {str(e)}\n")
    print(f"   💡 Certifique-se de que a Role '{ROLE_NAME}' existe no AWS Academy Lab\n")
    sys.exit(1)

# -- PASSO 5.2 - Criar ou Atualizar Glue Job

print(f"2️⃣  Criando/Atualizando Glue Job...\n")

job_ready = False
try:
    job_exists = False
    try:
        glue_client.get_job(JobName=JOB_NAME)
        print(f"   ℹ️  Job já existe. Atualizando...\n")
        job_exists = True
    except glue_client.exceptions.EntityNotFoundException:
        job_exists = False

    default_arguments = {
        "--job-bookmark-option": "job-bookmark-disable",
        "--enable-spark-ui": "true",
        "--spark-event-logs-path": f"s3://{BUCKET_NAME}/spark-logs/",
        "--enable-glue-datacatalog": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--BUCKET_NAME": BUCKET_NAME,
    }

    # create aceita Tags, update não
    create_job_config = {
        "Name": JOB_NAME,
        "Role": role_arn,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_s3_uri,
            "PythonVersion": "3",
        },
        "DefaultArguments": default_arguments,
        "GlueVersion": "5.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 30,
        "MaxRetries": 0,
        "Description": f"Gold: agrega workspace.tb_state_of_data_silver ({SCRIPT_FILE_NAME}) e cataloga a(s) tabela(s) Gold correspondente(s)",
        "Tags": {
            "Environment": "Development",
            "Project": "Tech-Challenge-Fase3-State-of-Data",
        },
    }

    update_job_config = {
        "Role": role_arn,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_s3_uri,
            "PythonVersion": "3",
        },
        "DefaultArguments": default_arguments,
        "GlueVersion": "5.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 30,
        "MaxRetries": 0,
        "Description": create_job_config["Description"],
    }

    if job_exists:
        glue_client.update_job(JobName=JOB_NAME, JobUpdate=update_job_config)
        print(f"   ✅ Job atualizado com sucesso!")
    else:
        glue_client.create_job(**create_job_config)
        print(f"   ✅ Job criado com sucesso!")

    print(f"\n   📊 Detalhes do Job:")
    print(f"      Nome: {JOB_NAME}")
    print(f"      Script: {script_s3_uri}")
    print(f"      Tipo Worker: G.1X")
    print(f"      Número de Workers: 2")
    print(f"      GlueVersion: 5.0")
    print(f"      Timeout: 30 minutos")
    print(f"      Role: {role_arn}\n")
    job_ready = True

except Exception as e:
    print(f"   ❌ Erro ao criar/atualizar job: {str(e)}\n")
    traceback.print_exc()

if not job_ready:
    sys.exit(1)

print(f"{'='*70}")
print("✅ Glue Job pronto!\n")

# -- PASSO 6 - Executar Glue Job (Run Job)


def print_cloudwatch_errors(job_run_id: str, max_lines: int = 80):
    # traceback do cloudwatch quando o job falha (o ErrorMessage vem cortado)
    candidates = [
        ("/aws-glue/jobs/error", job_run_id),
        ("/aws-glue/jobs/logs-v2", f"{job_run_id}-driver"),
        ("/aws-glue/jobs/output", job_run_id),
    ]
    for log_group, log_stream in candidates:
        try:
            resp = logs_client.get_log_events(
                logGroupName=log_group,
                logStreamName=log_stream,
                limit=2000,
                startFromHead=False,
            )
            events = resp.get("events", [])
            if not events:
                continue
            relevant = [
                ev["message"].rstrip()
                for ev in events
                if any(k in ev["message"] for k in ("Exception", "Error", "error", "Traceback", "Caused by"))
            ] or [ev["message"].rstrip() for ev in events]
            print(f"\n   📜 Logs ({log_group} / {log_stream}) - últimas {min(max_lines, len(relevant))} linhas relevantes:")
            for line in relevant[-max_lines:]:
                print(f"      {line}")
        except logs_client.exceptions.ResourceNotFoundException:
            continue
        except Exception as e:
            print(f"   ⚠️  Não foi possível ler {log_group}/{log_stream}: {str(e)}")


print("🚀 Executando Glue Job...\n")

final_state = None
try:
    print(f"⏳ Iniciando job: {JOB_NAME}...\n")

    response = glue_client.start_job_run(
        JobName=JOB_NAME,
        Arguments={
            "--BUCKET_NAME": BUCKET_NAME,
        },
    )

    job_run_id = response["JobRunId"]
    print(f"   ✅ Job iniciado com sucesso!")
    print(f"   📍 Job Run ID: {job_run_id}")
    print(f"   ⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # -- PASSO 6.1 - Monitorar o progresso do Job

    print(f"{'='*70}")
    print("📊 MONITORANDO PROGRESSO DO JOB")
    print(f"{'='*70}\n")

    max_wait_time = 1800  # 30 minutos (mesmo Timeout do job)
    check_interval = 10   # Verificar a cada 10 segundos
    elapsed_time = 0
    last_status = None

    while elapsed_time < max_wait_time:
        try:
            job_run = glue_client.get_job_run(JobName=JOB_NAME, RunId=job_run_id)

            job_status = job_run["JobRun"]
            state = job_status.get("JobRunState", "UNKNOWN")

            if state != last_status:
                elapsed_str = f"{elapsed_time//60}m {elapsed_time%60}s"

                state_icon = {
                    "STARTING": "⏳",
                    "RUNNING": "🔄",
                    "SUCCEEDED": "✅",
                    "FAILED": "❌",
                    "STOPPED": "⛔",
                    "TIMEOUT": "⏰",
                    "ERROR": "❌",
                }.get(state, "❓")

                print(f"{state_icon} {state:<15} | Tempo decorrido: {elapsed_str}", flush=True)
                last_status = state

            if state in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"]:
                final_state = state
                print(f"\n{'='*70}")
                print("📋 RESULTADO DO JOB")
                print(f"{'='*70}\n")

                print(f"   Status Final: {state}")
                print(f"   Run ID: {job_run_id}")
                print(f"   Start Time: {job_status.get('StartedOn', 'N/A')}")
                print(f"   End Time: {job_status.get('CompletedOn', 'N/A')}")
                print(f"   Execution Time: {job_status.get('ExecutionTime', 0)} s")
                print(f"   DPU Seconds: {job_status.get('DPUSeconds', 'N/A')}")

                if state == "SUCCEEDED":
                    print(f"\n   ✅ Job completado com sucesso!\n")
                else:
                    print(f"\n   ❌ Job finalizou com status: {state}\n")
                    print(f"   Error (ErrorMessage completo):")
                    print(f"   {job_status.get('ErrorMessage', 'N/A')}\n")
                    print_cloudwatch_errors(job_run_id)

                break

            time.sleep(check_interval)
            elapsed_time += check_interval

        except Exception as e:
            print(f"   ❌ Erro ao verificar status: {str(e)}")
            traceback.print_exc()
            break

    if elapsed_time >= max_wait_time:
        print(f"\n⏰ Timeout atingido ({max_wait_time} segundos)")
        print(f"   O job pode estar ainda em execução. Verifique no AWS Console.\n")

    # -- PASSO 6.2 - Exibir link do AWS Console

    print(f"{'='*70}")
    print("🔗 LINKS ÚTEIS")
    print(f"{'='*70}\n")

    console_url = f"https://console.aws.amazon.com/glue/home?region={AWS_REGION}#/jobs/view/{JOB_NAME}"
    print(f"   📍 Monitorar job no Console:")
    print(f"   {console_url}\n")

    cloudwatch_url = f"https://console.aws.amazon.com/cloudwatch/home?region={AWS_REGION}#logsV2:log-groups/aws-glue/jobs/{JOB_NAME}"
    print(f"   📊 Logs do CloudWatch:")
    print(f"   {cloudwatch_url}\n")

except Exception as e:
    print(f"❌ Erro ao executar job: {str(e)}\n")
    traceback.print_exc()

print(f"{'='*70}")
print("✅ Execução do Glue Job finalizada!\n")

if final_state != "SUCCEEDED":
    sys.exit(1)
