#!/usr/bin/env bash
# Crée un cluster Dataproc pour le réentraînement champion/challenger.
# Appelé automatiquement par le DAG Airflow fraud_retrain.
# Usage : bash create_retrain_cluster.sh CLUSTER_NAME
set -euo pipefail
source "$(dirname "$0")/../../.env"

CLUSTER_NAME="${1:?Usage: $0 CLUSTER_NAME}"

gcloud dataproc clusters create "${CLUSTER_NAME}" \
  --region="${GCP_REGION}" \
  --master-machine-type=n4-standard-4 \
  --master-boot-disk-type=hyperdisk-balanced \
  --master-boot-disk-size=80 \
  --num-workers=5 \
  --worker-machine-type=n4-standard-4 \
  --worker-boot-disk-type=hyperdisk-balanced \
  --worker-boot-disk-size=70 \
  --image-version=2.1-debian11 \
  --properties='spark:spark.jars.packages=com.microsoft.azure:synapseml_2.12:1.0.5' \
  --metadata="PIP_PACKAGES=synapseml==1.0.5 mlflow==2.16.2 google-cloud-storage gcsfs" \
  --initialization-actions="gs://${GCS_BUCKET}/init-actions/pip-install.sh" \
  --service-account="${GCP_SA}" \
  --max-idle=30m

echo "✅ Cluster retrain ${CLUSTER_NAME} prêt"
