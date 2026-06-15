#!/usr/bin/env bash
# Ouvre les ports nécessaires sur la VM.
# ⚠ Exécuter depuis Cloud Shell (pas depuis la VM — le SA n'a pas les rôles Compute).
set -euo pipefail
source "$(dirname "$0")/../../.env"

gcloud compute firewall-rules create allow-fraud-stack \
  --allow=tcp:5000,tcp:5601,tcp:8000,tcp:8080,tcp:9094 \
  --target-tags=fraud-vm \
  --description="MLflow(5000) Kibana(5601) fraud-api(8000) Airflow(8080) Kafka-ext(9094)" \
  2>/dev/null || echo "Règle firewall existe déjà"
echo "✅ Firewall configuré"
