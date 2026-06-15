#!/usr/bin/env bash
# reset_state.sh — Remet le pipeline dans l'état initial (v0).
# Utile entre deux simulations.
set -euo pipefail

echo "=== Reset de l'état du pipeline ==="

# 1. Supprimer l'index ES
echo "→ Suppression index fraud-predictions..."
curl -sf -X DELETE "http://localhost:9200/fraud-predictions" || echo "(absent, OK)"

# 2. Purger les dagRuns Airflow
echo "→ Purge des dagRuns Airflow..."
# L'API Airflow ne supporte pas DELETE sur tous les runs, on les laisse
# mais ils n'affectent pas la prochaine simulation.
echo "  (les anciens runs restent visibles dans l'UI mais n'interfèrent pas)"

# 3. Redémarrer fraud-api (reset compteur + reload modèle v0)
echo "→ Redémarrage fraud-api..."
docker restart fraud-api
sleep 15

echo ""
echo "✅ État réinitialisé. Prêt pour une nouvelle simulation."
echo "   Vérifier : curl localhost:8000/model-info"
