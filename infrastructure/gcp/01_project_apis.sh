#!/usr/bin/env bash
# Active les APIs GCP nécessaires au projet.
set -euo pipefail
source "$(dirname "$0")/../../.env"

gcloud config set project "${GCP_PROJECT_ID}"
gcloud services enable \
  compute.googleapis.com \
  dataproc.googleapis.com \
  storage.googleapis.com \
  artifactregistry.googleapis.com \
  iam.googleapis.com
echo "✅ APIs activées"
