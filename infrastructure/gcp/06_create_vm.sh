#!/usr/bin/env bash
# Crée la VM mlflow-server (e2-standard-4, 100 Go).
# ⚠ Exécuter depuis Cloud Shell.
set -euo pipefail
source "$(dirname "$0")/../../.env"

gcloud compute instances create "${VM_NAME}" \
  --zone="${GCP_ZONE}" \
  --machine-type="${VM_MACHINE_TYPE}" \
  --boot-disk-size="${VM_DISK_SIZE}GB" \
  --boot-disk-type=pd-balanced \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --service-account="${GCP_SA}" \
  --scopes=cloud-platform \
  --tags=fraud-vm \
  --metadata=startup-script='#!/bin/bash
    apt-get update -qq
    apt-get install -y -qq docker.io docker-compose
    systemctl enable docker
    usermod -aG docker $(whoami)'

echo "✅ VM ${VM_NAME} créée"
echo ""
echo "Étapes suivantes :"
echo "  1. Réserver l'IP statique :"
echo "     CURRENT_IP=\$(gcloud compute instances describe ${VM_NAME} --zone=${GCP_ZONE} --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
echo "     gcloud compute addresses create mlflow-server-static --addresses=\$CURRENT_IP --region=${GCP_REGION}"
echo "  2. SSH : gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE}"
echo "  3. Cloner le repo et lancer docker compose up -d"
