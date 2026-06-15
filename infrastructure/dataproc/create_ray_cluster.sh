#!/usr/bin/env bash
# Crée un cluster Dataproc pour Ray Train.
# Usage : bash create_ray_cluster.sh [CLUSTER_NAME] [NUM_WORKERS]
# Défaut : ray-bench, 5 workers
set -euo pipefail
source "$(dirname "$0")/../../.env"

CLUSTER_NAME="${1:-ray-bench}"
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
  --metadata="PIP_PACKAGES=mlflow==2.16.2 google-cloud-storage gcsfs" \
  --initialization-actions="gs://${GCS_BUCKET}/init-actions/pip-install.sh,gs://${GCS_BUCKET}/init-actions/ray-init.sh" \
  --service-account="${GCP_SA}" \
  --max-idle=30m

echo "✅ Cluster Ray ${CLUSTER_NAME} créé (${NUM_WORKERS} workers)"
echo ""
echo "Lancer le benchmark Ray (SSH sur le master) :"
echo "  gcloud compute ssh ${CLUSTER_NAME}-m --zone=${GCP_ZONE} --project=${GCP_PROJECT_ID}"
echo "  gsutil cp gs://${GCS_BUCKET}/code/bench_ray.py ~/"
echo "  sudo /opt/conda/default/bin/python ~/bench_ray.py"
echo ""
echo "⚠ Ray head tourne en root (init-action). JupyterLab kernel tourne en user différent."
echo "   → Préférer l'exécution en CLI (sudo python) plutôt que via notebook."
