# Résultats

Ce dossier stocke les exports MLflow et les comparaisons.

## Exporter les runs MLflow

```python
import mlflow
import pandas as pd

mlflow.set_tracking_uri("http://34.140.133.17:5000")

# Tous les runs du benchmark
runs = mlflow.search_runs(experiment_names=["ieee_fraud_lgbm"])
runs.to_csv("results/all_runs.csv", index=False)

# Benchmark Spark vs Ray uniquement
benchmark = mlflow.search_runs(
    experiment_names=["ieee_fraud_lgbm"],
    filter_string="tags.benchmark='spark_vs_ray_fair'"
)
benchmark.to_csv("results/spark_vs_ray.csv", index=False)
```

## Résultats confirmés

### Benchmark distribué (Track 2)

| Moteur | Config | Fit time | AUC-PR | AUC-ROC |
|--------|--------|----------|--------|---------|
| Spark SynapseML | 5 nodes | 866.0 s | 0.6508 | 0.9406 |
| Ray Train | 5 workers | 334.8 s (×2.59) | 0.6455 | 0.9384 |
| Ray Train | 1 node | 219.6 s (×3.94) | 0.6459 | 0.9375 |

### Pipeline temps réel (Track 3)

| Cycle | Modèle | AUC-PR | AUC-ROC |
|-------|--------|--------|---------|
| 0 (initial) | v0 | 0.5142 | 0.890 |
| 1 | v1 | — | — |
| 2 | v2 | — | — |
| 3 | v3 | — | — |
| 4 | v4 | 0.6001 | 0.927 |
