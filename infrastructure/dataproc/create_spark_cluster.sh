#!/usr/bin/env bash
# Crée un cluster Dataproc pour Spark + SynapseML.
# Usage : bash create_spark_cluster.sh [CLUSTER_NAME] [NUM_WORKERS]
# Défaut : fraud-spark-ray, 5 workers
set -euo pipefail
source "$(dirname "$0")/../../.env"

CLUSTER_NAME="${1:-fraud-spark-ray}"
NUM_WORKERS="${2:-5}"

gcloud dataproc clusters create "${CLUSTER_NAME}" \
  --region="${GCP_REGION}" \
  --master-machine-type=n4-standard-4 \
  --master-boot-disk-type=hyperdisk-balanced \
  --master-boot-disk-size=50 \
  --num-workers="${NUM_WORKERS}" \
  --worker-machine-type=n4-standard-4 \
  --worker-boot-disk-type=hyperdisk-balanced \
  --worker-boot-disk-size=50 \
  --image-version=2.1-debian11 \
  --properties='spark:spark.jars.packages=com.microsoft.azure:synapseml_2.12:1.0.5' \
  --metadata="PIP_PACKAGES=synapseml==1.0.5 mlflow==2.16.2 google-cloud-storage gcsfs" \
  --initialization-actions="gs://${GCS_BUCKET}/init-actions/pip-install.sh" \
  --service-account="${GCP_SA}" \
  --max-idle=30m

echo "✅ Cluster ${CLUSTER_NAME} créé (${NUM_WORKERS} workers)"
echo ""
echo "⚠ Quota Hyperdisk : $(( (NUM_WORKERS + 1) * 50 )) Go utilisés sur 500 Go max"
echo ""
echo "Soumettre le benchmark Spark :"
echo "  gcloud dataproc jobs submit pyspark gs://${GCS_BUCKET}/code/bench_spark.py \\"
echo "    --cluster=${CLUSTER_NAME} --region=${GCP_REGION} \\"
echo '    --properties="spark.jars.packages=com.microsoft.azure:synapseml_2.12:1.0.5,spark.dynamicAllocation.enabled=false,spark.executor.instances=6,spark.executor.cores=2,spark.task.cpus=2"'
echo ""
echo "Supprimer après usage :"
echo "  gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${GCP_REGION} --quiet"
