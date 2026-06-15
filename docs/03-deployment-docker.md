# Déploiement Docker

## Première installation (sur la VM)

```bash
# SSH
gcloud compute ssh mlflow-server --zone=europe-west1-b

# Cloner et configurer
git clone https://github.com/ben-simo/fraud-detection-mlops.git
cd fraud-detection-mlops
cp .env.example .env
nano .env   # Remplir KAFKA_CLUSTER_ID, AIRFLOW_ADMIN_PASSWORD, etc.

# Générer le KAFKA_CLUSTER_ID (une seule fois)
docker run --rm confluentinc/cp-kafka:7.6.1 kafka-storage random-uuid
# Copier l'UUID dans .env → KAFKA_CLUSTER_ID=...

# Permissions Airflow (UID 50000)
mkdir -p airflow/state airflow/logs
sudo chown -R 50000:0 airflow/state airflow/logs

# Lancer la stack
docker compose up -d

# Vérifier après ~2 min
bash ops/check_health.sh
```

## Race condition au boot

Tous les containers démarrent en parallèle (`restart: unless-stopped`). `fraud-api` peut tenter de se connecter à ES avant que la JVM ait fini de démarrer → `ensure_es_index()` échoue silencieusement → l'index `fraud-predictions` n'existe pas.

**Diagnostic :**
```bash
curl -s http://localhost:9200/fraud-predictions/_count
# Si 404 → race condition
docker logs fraud-api 2>&1 | grep -i "error\|connection"
```

**Fix :**
```bash
# Attendre qu'ES soit prêt
curl -s http://localhost:9200/_cluster/health
# Puis redémarrer fraud-api
docker restart fraud-api
# Attendre le log "🚀 Consumer prêt"
docker logs -f fraud-api
```

Le script `ops/check_health.sh` automatise cette détection et correction.

**Fix code-level** (production) : ajouter un retry/backoff dans `ensure_es_index()`.

## Mise à jour du code fraud-api

`fraud-api` n'a PAS de Dockerfile sur la VM actuelle — le code est monté en volume. `docker restart` ne suffit PAS pour prendre en compte des changements de code. Il faut :

```bash
docker stop fraud-api && docker rm fraud-api
# Puis relancer avec la commande docker run complète
# (récupérer depuis bash history : grep "docker run" ~/.bash_history | grep fraud-api)
```

Avec le `docker-compose.yml` de ce repo, un simple `docker compose up -d --build fraud-api` suffit.

## Services et ports

| Service | URL interne | URL externe |
|---------|-------------|-------------|
| MLflow | http://mlflow-server:5000 | http://34.140.133.17:5000 |
| Elasticsearch | http://elasticsearch:9200 | http://34.140.133.17:9200 |
| Kibana | http://kibana:5601 | http://34.140.133.17:5601 |
| Kafka | kafka:9092 | 34.140.133.17:9094 |
| Airflow | http://airflow:8080 | http://34.140.133.17:8080 |
| fraud-api | http://fraud-api:8000 | http://34.140.133.17:8000 |

## Elasticsearch mono-node

Le cluster ES tourne en mono-node. Toujours configurer `number_of_replicas: 0` :

```bash
curl -X PUT "localhost:9200/_all/_settings" -H 'Content-Type: application/json' -d '
  {"index": {"number_of_replicas": 0}}'
```

Si le disque se remplit → ES passe en `read_only_allow_delete`. Après nettoyage :
```bash
curl -X PUT "localhost:9200/_all/_settings" -H 'Content-Type: application/json' -d '
  {"index.blocks.read_only_allow_delete": null}'
curl -X POST "localhost:9200/_cluster/reroute?retry_failed=true"
```
