# Notebooks

Les notebooks centralisés tournent sur Kaggle. Exporter depuis votre profil Kaggle
et les placer ici.

## Liste des notebooks

| # | Fichier | Description |
|---|---------|-------------|
| 00 | `common-preprocessing.ipynb` | Prétraitement commun (tous modèles) |
| 01 | `tree-models-baseline.ipynb` | RF/XGBoost/LightGBM × 3 stratégies rééquilibrage |
| 02 | `neural-tabular.ipynb` | DNN / TabTransformer / FT-Transformer |
| 03 | `gnn-bipartite.ipynb` | GraphSAGE (DGL) |
| 04 | `lightgbm-pca.ipynb` | LightGBM + PCA |
| 05 | `lightgbm-cv-temporal.ipynb` | LightGBM + validation croisée temporelle |
| 06 | `lightgbm-optuna.ipynb` | LightGBM + Optuna (60 trials) |
| 07 | `export-parquet-splits.ipynb` | Export dataset préprocessé (80/20) → GCS |

## Commande Kaggle

```bash
# Installer l'API Kaggle
pip install kaggle
# Configurer ~/.kaggle/kaggle.json

# Télécharger le dataset
kaggle competitions download -c ieee-fraud-detection
```

## Métriques attendues (meilleurs modèles centralisés)

| Modèle | AUC-PR | AUC-ROC | Temps |
|--------|--------|---------|-------|
| LightGBM baseline | 0.5839 | 0.9138 | — |
| XGBoost baseline | 0.5768 | 0.9082 | — |
| LightGBM + Optuna | ~0.60 | ~0.92 | — |
| FT-Transformer | ~0.45 | ~0.85 | GPU |
| GraphSAGE | ~0.38 | ~0.80 | GPU |
