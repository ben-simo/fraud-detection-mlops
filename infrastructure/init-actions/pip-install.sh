#!/usr/bin/env bash
# Init-action Dataproc : installe les libs Python sur tous les nœuds.
# Poussé sur gs://<bucket>/init-actions/pip-install.sh
set -euo pipefail

PIP_PACKAGES=$(/usr/share/google/get_metadata_value attributes/PIP_PACKAGES || true)
if [[ -n "${PIP_PACKAGES}" ]]; then
  echo "Installing: ${PIP_PACKAGES}"
  pip install --upgrade ${PIP_PACKAGES}
fi
