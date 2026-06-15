# Reproduire les Expériences

## Track 1 — Modèles centralisés (Kaggle)

Les notebooks centralisés tournent sur Kaggle (GPU P100 pour les modèles neuronaux).

### Notebooks à récupérer depuis Kaggle

| # | Notebook | Contenu |
|---|----------|---------|
| 00 | `common-preprocessing.ipynb` | Reduce memory, merge, drop >90% NaN, normalisation D, features temporelles, TransactionAmt, email, M-agrégats, device split → pickle |
| 01 | `tree-models-baseline.ipynb` | UID, frequency encoding, magic features, ratio/zscore, label encoding, imputation -999, split 60/20/20. RF/XGBoost/LightGBM × {baseline, SMOTE, k-means+AG} |
| 02 | `neural-tabular.ipynb` | DNN vs TabTransformer vs FT-Transformer (GPU Kaggle) |
| 03 | `gnn-bipartite.ipynb` | GraphSAGE (DGL, 2 HeteroGraphConv, hidden 128) |
| 04 | `lightgbm-pca.ipynb` | LightGBM + PCA |
| 05 | `lightgbm-cv-temporal.ipynb` | LightGBM + validation croisée temporelle |
| 06 | `lightgbm-optuna.ipynb` | LightGBM + Optuna (TPE, MedianPruner, 60 trials) |
| 07 | `export-parquet-splits.ipynb` | Exporte le dataset préprocessé en parquet pour le benchmark distribué |

### Métriques de comparaison

- AUC-ROC (métrique officielle Kaggle IEEE-CIS)
- AUC-PR (métrique primaire du projet — adaptée aux classes rares)
- Temps d'entraînement (`%time` dans les cellules Kaggle)

### Distinction importante : `is_unbalance` vs SMOTE

- `is_unbalance=True` : **repondération** — modifie la fonction de perte, pas les données
- SMOTE : **rééchantillonnage** — modifie la distribution des données (synthèse de nouveaux exemples)

Ce sont deux approches conceptuellement distinctes du déséquilibre de classes.

---

## Track 2 — Benchmark distribué (GCP Dataproc)

### Principe

Comparer uniquement le **moteur d'entraînement** (fit). Le preprocessing est fait une fois en amont → un parquet « prêt à entraîner » (464 features numériques + `isFraud`). Les deux moteurs lisent le MÊME parquet, MÊMES hyperparamètres, MÊME nombre de workers (5), MÊME nombre de threads/worker (2).

### Dataset préprocessé

Généré par `export-parquet-splits.ipynb` (notebook 07) :
```
gs://pfe-fraud-dataproc-bucket/features/fraud_train_proc.parquet   (80%, 472 432 rows)
gs://pfe-fraud-dataproc-bucket/features/fraud_test_proc.parquet    (20%, 118 108 rows)
```

### Lancer le benchmark Spark

```bash
# 1. Créer le cluster
bash infrastructure/dataproc/create_spark_cluster.sh fraud-spark-ray 5

# 2. Soumettre le job
gcloud dataproc jobs submit pyspark gs://pfe-fraud-dataproc-bucket/code/bench_spark.py \
  --cluster=fraud-spark-ray --region=europe-west1 \
  --properties="spark.jars.packages=com.microsoft.azure:synapseml_2.12:1.0.5,\
spark.dynamicAllocation.enabled=false,\
spark.executor.instances=6,\
spark.executor.cores=2,\
spark.task.cpus=2"

# ⚠ NE PAS interrompre — attendre ">>> MLflow loggé OK."

# 3. Supprimer le cluster
gcloud dataproc clusters delete fraud-spark-ray --region=europe-west1 --quiet
```

### Lancer le benchmark Ray

```bash
# 1. Créer le cluster (init-action ray-init.sh)
bash infrastructure/dataproc/create_ray_cluster.sh ray-bench 5

# 2. SSH sur le master et exécuter
gcloud compute ssh ray-bench-m --zone=europe-west1-b
gsutil cp gs://pfe-fraud-dataproc-bucket/code/bench_ray.py ~/
sudo /opt/conda/default/bin/python ~/bench_ray.py

# ⚠ Exécuter en root (sudo) — le head Ray tourne en root via init-action.
# ⚠ Ne PAS utiliser JupyterLab (conflit de permissions /tmp/ray).

# 3. Supprimer le cluster
gcloud dataproc clusters delete ray-bench --region=europe-west1 --quiet
```

### Résultats confirmés

| Moteur | Config | Fit time | AUC-PR | AUC-ROC |
|--------|--------|----------|--------|---------|
| Spark SynapseML | 5 nodes, 2 threads | 866.0 s | 0.6508 | 0.9406 |
| Ray Train | 5 workers, 2 CPUs | 334.8 s (×2.59) | 0.6455 | 0.9384 |
| Ray Train | 1 node | 219.6 s (×3.94) | 0.6459 | 0.9375 |

⚠ **Ne pas comparer Track 1 et Track 2** : feature engineering différent (Track 1 : ~425 features, AUC-PR ~0.51–0.60 ; Track 2 : 464 features, AUC-PR ~0.65).

### Comparer les runs MLflow

```python
import mlflow
mlflow.set_tracking_uri("http://34.140.133.17:5000")
runs = mlflow.search_runs(
    experiment_names=["ieee_fraud_lgbm"],
    filter_string="tags.benchmark='spark_vs_ray_fair'"
)
print(runs[["tags.engine", "metrics.time_train_lgbm_s", "metrics.test_auc_pr", "metrics.test_auc_roc"]])
```

---

## Track 3 — Pipeline temps réel + réentraînement

### Lancer la simulation complète

```bash
# 1. Vérifier la santé de la stack
bash ops/check_health.sh

# 2. Reset (si nécessaire — entre deux simulations)
bash ops/reset_state.sh

# 3. Lancer (dans tmux pour survivre à une déconnexion SSH)
tmux new -s simulation
bash ops/start_simulation.sh 10    # 10 tx/s → ~3h17 pour 118 108 tx

# 4. Surveiller
# Terminal 2 :
watch -n 30 'curl -s localhost:9200/fraud-predictions/_count?pretty'
# Terminal 3 :
docker logs -f fraud-api 2>&1 | grep -E "cycle|retrain|promote|reload"
# Airflow UI : http://34.140.133.17:8080
# Kibana : http://34.140.133.17:5601
```

### Résultats attendus (simulation complète)

- 118 108 transactions en ~3h17 (10 tx/s)
- 4 cycles de réentraînement automatiques (seuil : 29 527 tx)
- 4/4 challengers promus (v0 → v1 → v2 → v3 → v4)
- AUC-PR : 0.5142 (v0) → 0.6001 (v4) — gain +16.7%
- AUC-ROC : 0.890 → 0.927
- Latence serving : moyenne 1.25 ms, p99 2.55 ms
- Coût Dataproc estimé : ~$1.74 pour 4 retrains

### Preuve de l'auto-trigger

Les `dag_run_id` suivent le pattern `fraudapi__cycleN__<timestamp>`, prouvant que le trigger provient de fraud-api (pas d'intervention manuelle).

---

## Kibana — Dashboards

### Importer

1. Exporter les dashboards depuis Kibana : Stack Management → Saved Objects → Export
2. Sauvegarder dans `kibana/dashboards.ndjson`
3. Réimporter : Stack Management → Saved Objects → Import

### Points d'attention

- Time picker : passer à "Today" ou range absolu (pas "Last 15 minutes" !)
- `typeMigrationVersion` : doit être `8.5.0` (pas `8.8.0`) pour Kibana 8.13
- Deux data views peuvent coexister (Default + sans Default) → ajouter les runtime fields aux deux
- `model_label` : runtime field Painless pour afficher v0/v1/v2/v3/v4
- `*.pct` : ratios 0–1, utiliser le format pourcentage dans les panels
- Dark mode : Stack Management → Advanced Settings → Dark mode → On → F5
