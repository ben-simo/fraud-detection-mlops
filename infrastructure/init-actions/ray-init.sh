#!/usr/bin/env bash
# Init-action Dataproc : installe et lance Ray sur chaque nœud.
# Poussé sur gs://<bucket>/init-actions/ray-init.sh
#
# Architecture :
#   - Master : ray start --head --num-cpus=0 (pas de calcul, coordination seule)
#   - Workers : ray start --address=<master>:6379
#
# Connexion depuis un script : ray.init(address="<cluster>-m:6379")
# Exécuter en root (le head tourne en root via init-action).
set -euo pipefail

RAY_VERSION="2.55.1"
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

pip install "ray[default]==${RAY_VERSION}" lightgbm gcsfs

if [[ "${ROLE}" == "Master" ]]; then
  ray start --head --port=6379 --num-cpus=0
  echo "✅ Ray head started on ${MASTER}:6379"
else
  # Attendre que le head soit prêt
  for i in $(seq 1 30); do
    ray start --address="${MASTER}:6379" && break
    echo "Waiting for head... (attempt ${i})"
    sleep 5
  done
fi
