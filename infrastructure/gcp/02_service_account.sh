#!/usr/bin/env bash
# Crée le service account et attribue les rôles nécessaires.
set -euo pipefail
source "$(dirname "$0")/../../.env"

SA_NAME="fraud-pipeline-sa"

gcloud iam service-accounts create "${SA_NAME}" \
  --display-name="Fraud Pipeline SA" \
  --project="${GCP_PROJECT_ID}" 2>/dev/null || echo "SA existe déjà"

for ROLE in \
  roles/dataproc.editor \
  roles/dataproc.worker \
  roles/storage.admin \
  roles/artifactregistry.writer \
  roles/logging.logWriter \
  roles/monitoring.metricWriter; do
  gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member="serviceAccount:${GCP_SA}" \
    --role="${ROLE}" --quiet
done
echo "✅ SA configuré : ${GCP_SA}"

# ⚠ Le SA n'a PAS les rôles Compute Engine.
# Les commandes gcloud compute * doivent être exécutées depuis Cloud Shell
# (credentials owner), pas depuis la VM.
