#!/usr/bin/env bash
# start_simulation.sh — Reset complet + lancement de la simulation.
#
# Usage :
#   bash ops/start_simulation.sh [RATE]    (défaut: 10 tx/s)
#
# Actions :
#   1. Vérifie qu'aucun producer n'est déjà actif
#   2. Supprime l'index ES fraud-predictions (reset des données)
#   3. Redémarre fraud-api (reset du compteur in-memory total_tx)
#   4. Attend que fraud-api soit prêt (log "🚀 Consumer prêt")
#   5. Lance producer_kafka.py dans le terminal actuel
#
# ⚠ NE PAS redémarrer fraud-api pendant la simulation !
#   Cela réinitialise total_tx et casse les seuils de retrain.
set -euo pipefail

RATE="${1:-10}"

# Guard: pas de double producer
if pgrep -f "producer_kafka" > /dev/null 2>&1; then
  echo "❌ Un producer est déjà actif. Arrêtez-le d'abord."
  exit 1
fi

echo "=== Reset & Simulation — rate=${RATE} tx/s ==="

# 1. Delete ES index
echo "→ Suppression de l'index fraud-predictions..."
curl -sf -X DELETE "http://localhost:9200/fraud-predictions" || echo "(index absent, OK)"

# 2. Restart fraud-api
echo "→ Redémarrage de fraud-api (reset compteurs)..."
docker restart fraud-api

# 3. Wait for fraud-api ready
echo "→ Attente de fraud-api..."
for i in $(seq 1 60); do
  if docker logs fraud-api 2>&1 | tail -5 | grep -q "Consumer prêt"; then
    echo "✅ fraud-api prêt"
    break
  fi
  sleep 2
done

# 4. Verify index created
curl -sf "http://localhost:9200/fraud-predictions/_count?pretty" || echo "⚠ Index pas encore créé"

# 5. Launch producer
echo ""
echo "=== Lancement du producer (${RATE} tx/s) ==="
echo "    Pour la simulation complète (~3h17 à 10 tx/s) :"
echo "    → Utiliser tmux pour survivre à une déconnexion SSH"
echo "    → tmux new -s simulation"
echo ""
python3 streaming/producer_kafka.py --rate "${RATE}"
