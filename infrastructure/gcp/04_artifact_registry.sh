#!/usr/bin/env bash
# Crée le dépôt Artifact Registry pour les images Docker.
set -euo pipefail
source "$(dirname "$0")/../../.env"

gcloud artifacts repositories create fraud-images \
  --repository-format=docker \
  --location="${GCP_REGION}" \
  --description="Images Docker du pipeline fraude" \
  2>/dev/null || echo "Registry existe déjà"
echo "✅ Registry : ${ARTIFACT_REGISTRY}"
