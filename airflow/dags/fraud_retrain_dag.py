"""
fraud_retrain_dag.py — DAG Airflow : réentraînement champion/challenger

Workflow :
  1. create_cluster     → Crée un cluster Dataproc (5 workers, SynapseML)
  2. submit_retrain     → Soumet pipeline_retrain.py (entraîne challenger sur
                           les données ES, compare avec champion)
  3. read_result        → Lit result.json depuis GCS
  4. decide_promotion   → Branch : promote_and_reload OU skip_reload
  5. promote_and_reload → Met à jour current_champion.json + /reload fraud-api
     OU skip_reload     → Rien à faire (challenger < champion)
  6. cleanup_cluster    → Supprime le cluster (toujours exécuté)

Trigger :
  - Automatique : fraud-api POST /api/v1/dags/fraud_retrain/dagRuns
    avec conf={"cycle": N} toutes les BATCH_PER_CYCLE transactions.
  - Manuel : depuis l'UI Airflow avec conf={"cycle": 1}

⚠ SequentialExecutor : le scheduler peut afficher des heartbeat warnings
  pendant submit_retrain (~30 min de job Dataproc). C'est attendu, pas une erreur.
"""
import json
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.dates import days_ago

# ─────────────────── CONFIG ───────────────────
GCP_PROJECT   = "pfe-fraud-detection"
GCP_REGION    = "europe-west1"
GCS_BUCKET    = "pfe-fraud-dataproc-bucket"
CLUSTER_NAME  = "fraud-retrain-cluster"
SA            = "fraud-pipeline-sa@pfe-fraud-detection.iam.gserviceaccount.com"
MLFLOW_URI    = "http://34.140.133.17:5000"
FRAUD_API_URL = "http://fraud-api:8000"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n4-standard-4",
        "disk_config": {"boot_disk_type": "hyperdisk-balanced", "boot_disk_size_gb": 80},
    },
    "worker_config": {
        "num_instances": 5,
        "machine_type_uri": "n4-standard-4",
        "disk_config": {"boot_disk_type": "hyperdisk-balanced", "boot_disk_size_gb": 70},
    },
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "spark:spark.jars.packages": "com.microsoft.azure:synapseml_2.12:1.0.5",
        },
    },
    "initialization_actions": [
        {"executable_file": f"gs://{GCS_BUCKET}/init-actions/pip-install.sh"},
    ],
    "gce_cluster_config": {
        "service_account": SA,
        "metadata": {
            "PIP_PACKAGES": "synapseml==1.0.5 mlflow==2.16.2 google-cloud-storage gcsfs",
        },
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": GCP_PROJECT},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/code/pipeline_retrain.py",
        "args": ["--cycle", "{{ dag_run.conf.get('cycle', '1') }}"],
        "properties": {
            "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:1.0.5",
            "spark.dynamicAllocation.enabled": "false",
            "spark.executor.instances": "6",
            "spark.executor.cores": "2",
            "spark.task.cpus": "2",
        },
    },
}


# ─────────────────── CALLBACKS ───────────────────
def read_result(**context):
    """Lit result.json depuis GCS et le pousse en XCom."""
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(GCS_BUCKET).blob("models/result.json")
    result = json.loads(blob.download_as_text())
    context["ti"].xcom_push(key="result", value=result)
    print(f">>> Result: {result}")


def decide_promotion(**context):
    """Branch : promouvoir le challenger ou non."""
    result = context["ti"].xcom_pull(key="result", task_ids="read_result")
    if result.get("promoted", False):
        return "promote_and_reload"
    return "skip_reload"


def promote_and_reload(**context):
    """Met à jour le pointeur champion et recharge fraud-api."""
    import requests
    result = context["ti"].xcom_pull(key="result", task_ids="read_result")
    # Le pointeur current_champion.json est déjà écrit par pipeline_retrain.py
    # On recharge fraud-api pour qu'il charge le nouveau modèle
    try:
        resp = requests.post(f"{FRAUD_API_URL}/reload", timeout=30)
        print(f">>> Reload fraud-api : {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"⚠ Reload échoué (fraud-api pas accessible depuis Airflow?) : {e}")


# ─────────────────── DAG ───────────────────
default_args = {
    "owner": "ben",
    "retries": 0,
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="fraud_retrain",
    default_args=default_args,
    description="Réentraînement champion/challenger LightGBM distribué",
    schedule_interval=None,  # trigger uniquement (auto ou manuel)
    start_date=days_ago(1),
    catchup=False,
    tags=["fraud", "retrain", "champion-challenger"],
) as dag:

    create = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
        labels={"pipeline": "fraud-retrain"},
    )

    submit = DataprocSubmitJobOperator(
        task_id="submit_retrain",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job=PYSPARK_JOB,
    )

    read = PythonOperator(task_id="read_result", python_callable=read_result)

    decide = BranchPythonOperator(task_id="decide_promotion", python_callable=decide_promotion)

    promote = PythonOperator(task_id="promote_and_reload", python_callable=promote_and_reload)

    skip = EmptyOperator(task_id="skip_reload")

    cleanup = DataprocDeleteClusterOperator(
        task_id="cleanup_cluster",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",  # supprime même si le retrain échoue
    )

    create >> submit >> read >> decide >> [promote, skip] >> cleanup
