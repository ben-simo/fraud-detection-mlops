#!/usr/bin/env bash
# Crée le bucket GCS et la structure de dossiers.
set -euo pipefail
source "$(dirname "$0")/../../.env"

gsutil mb -p "${GCP_PROJECT_ID}" -l "${GCP_REGION}" \
  "gs://${GCS_BUCKET}/" 2>/dev/null || echo "Bucket existe déjà"

# Structure attendue dans le bucket :
for DIR in code features raw/splits init-actions models ray_results extra; do
  gsutil cp /dev/null "gs://${GCS_BUCKET}/${DIR}/.gitkeep" 2>/dev/null || true
done
echo "✅ Bucket gs://${GCS_BUCKET}/ prêt"
echo "Structure :"
echo "  code/          → bench_spark.py, bench_ray.py, pipeline_retrain.py"
echo "  features/      → fraud_train_proc.parquet, fraud_test_proc.parquet"
echo "  raw/splits/    → train.parquet, val.parquet, test.parquet (60/20/20)"
echo "  init-actions/  → pip-install.sh, ray-init.sh"
echo "  models/        → current_champion.json, lgb_model_vN.txt"
echo "  ray_results/   → Ray Train checkpoints"
