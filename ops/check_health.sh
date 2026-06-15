#!/usr/bin/env bash
# check_health.sh — Vérifie la santé de tous les containers.
# Gère la race condition au boot : si fraud-api n'a pas créé l'index ES,
# attend que ES soit prêt puis redémarre fraud-api.
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "${GREEN}✅ $1${NC}"; }
warn() { echo -e "${YELLOW}⚠  $1${NC}"; }
fail() { echo -e "${RED}❌ $1${NC}"; }

echo "=== Health Check — $(date) ==="
echo ""

# 1. Containers running?
for C in mlflow-server elasticsearch kibana kafka airflow fraud-api metricbeat; do
  STATUS=$(docker inspect -f '{{.State.Status}}' "$C" 2>/dev/null || echo "missing")
  if [[ "$STATUS" == "running" ]]; then
    ok "$C running"
  else
    fail "$C: $STATUS"
  fi
done
echo ""

# 2. Elasticsearch cluster health
ES_HEALTH=$(curl -sf "http://localhost:9200/_cluster/health" 2>/dev/null || echo '{"status":"unreachable"}')
ES_STATUS=$(echo "$ES_HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo "?")
if [[ "$ES_STATUS" == "green" || "$ES_STATUS" == "yellow" ]]; then
  ok "Elasticsearch: $ES_STATUS"
else
  fail "Elasticsearch: $ES_STATUS"
fi

# 3. Index fraud-predictions exists?
INDEX_COUNT=$(curl -sf "http://localhost:9200/fraud-predictions/_count" 2>/dev/null | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo "0")

if [[ "$INDEX_COUNT" == "0" ]]; then
  warn "Index fraud-predictions absent ou vide"
  echo "   → Race condition probable. Redémarrage de fraud-api..."

  # Attendre ES ready
  for i in $(seq 1 30); do
    if curl -sf "http://localhost:9200/_cluster/health" > /dev/null 2>&1; then
      break
    fi
    echo "   Waiting for ES... (${i}/30)"
    sleep 2
  done

  docker restart fraud-api
  echo "   Attente de fraud-api (log '🚀 Consumer prêt')..."
  sleep 10
  # Vérifier
  NEW_STATUS=$(curl -sf "http://localhost:9200/fraud-predictions/_count" 2>/dev/null || echo "still absent")
  ok "fraud-api redémarré — index: $NEW_STATUS"
else
  ok "Index fraud-predictions : $INDEX_COUNT docs"
fi

echo ""
# 4. Services web
for URL in "http://localhost:5000 MLflow" "http://localhost:5601 Kibana" \
           "http://localhost:8000/health fraud-api" "http://localhost:8080 Airflow"; do
  ADDR=$(echo "$URL" | cut -d' ' -f1)
  NAME=$(echo "$URL" | cut -d' ' -f2)
  if curl -sf "$ADDR" > /dev/null 2>&1; then
    ok "$NAME accessible"
  else
    fail "$NAME inaccessible ($ADDR)"
  fi
done
