# Troubleshooting

## Problèmes fréquents

### 1. fraud-api ne crée pas l'index ES après un boot

**Symptôme** : `curl localhost:9200/fraud-predictions/_count` → 404  
**Cause** : Race condition — fraud-api démarre avant ES  
**Fix** :
```bash
# Attendre ES
curl -s localhost:9200/_cluster/health
# Redémarrer fraud-api
docker restart fraud-api
# Vérifier
docker logs -f fraud-api  # attendre "🚀 Consumer prêt"
```

### 2. SynapseML barrier mode sur Dataproc

| Symptôme | Cause | Fix |
|----------|-------|-----|
| `BarrierJobRunWithDynamicAllocationException` [SPARK-24942] | Allocation dynamique activée (défaut Dataproc) | `spark.dynamicAllocation.enabled=false` |
| `requires 5 slots, but only 4 available` [SPARK-24819] | Exécuteurs fixes insuffisants | `spark.executor.instances=6,executor.cores=2,task.cpus=2` |
| 5 exécuteurs de 4 cœurs ne tiennent pas | L'Application Master consomme ~1 cœur | Rester à 2 threads/worker ou ajouter un 6e worker |

### 3. Ray : permission denied dans JupyterLab

**Symptôme** : `Permission denied /tmp/ray/session_*/logs` + SIGABRT  
**Cause** : Ray head tourne en root (init-action), Jupyter kernel en user différent  
**Fix** : Exécuter en CLI avec `sudo /opt/conda/default/bin/python ~/bench_ray.py`  
**Alternative** : Configurer le kernel pour tourner en root

### 4. Airflow 403 Forbidden sur l'API REST

**Symptôme** : `POST /api/v1/dags/fraud_retrain/dagRuns` → 403  
**Cause** : Backend d'authentification par défaut = session only  
**Fix** : Ajouter dans l'environnement Airflow :
```
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session
```
Puis `docker restart airflow`.

### 5. Elasticsearch disque plein

**Symptôme** : Index en `read_only_allow_delete`  
**Cause** : Indexation de toutes les colonnes du dataset (30 Go insuffisants)  
**Fix** :
```bash
# Libérer de l'espace ou agrandir le disque (30 → 100 Go)
# Puis débloquer
curl -X PUT "localhost:9200/_all/_settings" \
  -H 'Content-Type: application/json' \
  -d '{"index.blocks.read_only_allow_delete": null}'
curl -X POST "localhost:9200/_cluster/reroute?retry_failed=true"
```
**Prévention** : N'indexer que les colonnes utiles à Kibana. Les autres colonnes en simple référence pour collecte GCS (réentraînement).

### 6. Airflow scheduler heartbeat warnings

**Symptôme** : Warnings "Scheduler heartbeat" pendant `submit_retrain`  
**Cause** : SequentialExecutor bloque sur un job Dataproc de ~30 min  
**Statut** : Attendu, pas une erreur. Le DAG terminera correctement.

### 7. Metricbeat "too old" API warnings

**Symptôme** : Logs Metricbeat avec "API version too old"  
**Fix** : Ajouter `DOCKER_API_VERSION=1.44` dans l'environnement Metricbeat  
**Vérification** :
```bash
sleep 20 && docker logs metricbeat 2>&1 | grep -ic "too old"
# Doit retourner 0
```

### 8. Metricbeat permissions

**Symptôme** : `Operation not permitted` sur `chown`/`chmod`  
**Fix** (commande correcte — les deux avec `sudo`) :
```bash
sudo chown root:root ~/metricbeat.yml && sudo chmod go-w ~/metricbeat.yml
```

### 9. Hyperdisk quota exceeded

**Symptôme** : `HDB_TOTAL_GB exceeded` à la création du cluster  
**Cause** : Deux clusters en parallèle (6×50 + 6×50 > 500 Go)  
**Fix** : Supprimer le cluster inutilisé avant d'en créer un autre  
```bash
gcloud dataproc clusters delete <nom> --region=europe-west1 --quiet
```

### 10. fraud-api redémarré pendant la simulation

**Symptôme** : Les seuils de retrain ne se déclenchent plus  
**Cause** : `docker restart` réinitialise le compteur `total_tx` en mémoire  
**Fix** : NE JAMAIS redémarrer fraud-api pendant une simulation. Reset complet nécessaire.

## Commandes de diagnostic

```bash
# Santé globale
bash ops/check_health.sh

# Logs d'un container
docker logs -f <container> 2>&1 | tail -50

# État ES
curl -s localhost:9200/_cluster/health?pretty
curl -s localhost:9200/_cat/indices?v

# Modèle actuel
curl -s localhost:8000/model-info

# Stats fraud-api
curl -s localhost:8000/stats

# Runs MLflow
curl -s localhost:5000/api/2.0/mlflow/experiments/list

# Airflow DAG
curl -s -u admin:admin localhost:8080/api/v1/dags/fraud_retrain | python3 -m json.tool
```
