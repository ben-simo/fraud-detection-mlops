#!/usr/bin/env bash
# Promeut l'IP éphémère de la VM en IP statique.
# ⚠ EXÉCUTER AVANT TOUT STOP/START DE LA VM (sinon l'IP change et casse tout).
# ⚠ Exécuter depuis Cloud Shell.
set -euo pipefail
source "$(dirname "$0")/../../.env"

CURRENT_IP=$(gcloud compute instances describe "${VM_NAME}" \
  --zone="${GCP_ZONE}" \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

echo "IP actuelle : ${CURRENT_IP}"

gcloud compute addresses create mlflow-server-static \
  --addresses="${CURRENT_IP}" \
  --region="${GCP_REGION}"

gcloud compute addresses list --regions="${GCP_REGION}"
echo "✅ IP statique réservée : ${CURRENT_IP}"
