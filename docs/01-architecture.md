# Architecture du Pipeline

## Architecture Kappa — 5 étapes canoniques

Le pipeline suit l'architecture Kappa (pas de couche batch séparée). Tout passe par le flux temps réel.

```
Source de données   →   Journal immuable   →   Stream processing   →   Vues matérialisées   →   Tableau de bord
  (producer.py)          (Kafka KRaft)        (fraud-api FastAPI)     (Elasticsearch)          (Kibana)
```

### Composants

| Composant | Conteneur | Port | Rôle |
|-----------|-----------|------|------|
| MLflow | `mlflow-server` | 5000 | Tracking des expériences, registre des modèles |
| Elasticsearch | `elasticsearch` | 9200 | Indexation des prédictions, mono-node (replicas=0) |
| Kibana | `kibana` | 5601 | Dashboards temps réel |
| Kafka | `kafka` | 9092 (int) / 9094 (ext) | Journal immuable, mode KRaft (sans Zookeeper) |
| Airflow | `airflow` | 8080 | Orchestration DAG retrain (SequentialExecutor + SQLite) |
| fraud-api | `fraud-api` | 8000 | Scoring LightGBM temps réel, trigger retrain |
| Metricbeat | `metricbeat` | — | Métriques système + Docker → ES |

### Flux de données

1. `producer_kafka.py` lit `val.parquet` et envoie les transactions au topic Kafka `fraud-transactions`.
2. `fraud-api` consomme le topic, score chaque transaction avec LightGBM, et indexe le résultat (prédiction, latence, `model_version`) dans l'index ES `fraud-predictions`.
3. Tous les `BATCH_PER_CYCLE` transactions (29 527), `fraud-api` POST l'API REST Airflow pour déclencher le DAG `fraud_retrain`.
4. Le DAG crée un cluster Dataproc, entraîne un challenger sur une fenêtre cumulative des données ES, compare avec le champion, et promeut si meilleur.
5. Si promotion : `fraud-api` recharge le nouveau modèle via `/reload`.

### Mécanisme champion/challenger

- Le champion courant est pointé par `gs://<bucket>/models/current_champion.json`
- Chaque cycle entraîne un challenger sur un jeu augmenté (fenêtre cumulative : `tx_seq <= cycle * BATCH_PER_CYCLE`)
- Si `challenger.auc_pr > champion.auc_pr` → promotion + hot-swap du modèle
- Résultat de la simulation : 4/4 challengers promus (v0→v1→v2→v3→v4)

### Réseau Docker

Tous les containers tournent sur le réseau `mlflow_pfe-net`. Les noms d'hôte internes (ex: `kafka:9092`, `elasticsearch:9200`) sont utilisés pour la communication inter-services.

## Infrastructure GCP

- **Projet** : `pfe-fraud-detection`, région `europe-west1`
- **VM** : `mlflow-server`, e2-standard-4, 100 Go, IP statique `34.140.133.17`
- **GCS** : `gs://pfe-fraud-dataproc-bucket/`
- **Dataproc** : Clusters éphémères (créés/supprimés par le DAG)
- **Service Account** : `fraud-pipeline-sa` — n'a PAS les rôles Compute Engine
  - Les commandes `gcloud compute *` doivent être exécutées depuis Cloud Shell (owner)
