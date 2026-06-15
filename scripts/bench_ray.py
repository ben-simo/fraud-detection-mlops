#!/usr/bin/env python3
"""
BENCHMARK RAY — Ray Train LightGBMTrainer (5 workers x 2 CPUs)

Comparaison équitable vs Spark : MÊME parquet préprocessé, MÊMES hyperparamètres,
MÊME nb workers (5), MÊME CPUs/worker (2), on chronomètre SEUL le fit.

EXÉCUTION (SSH sur le master ray-bench) :
  gcloud compute ssh ray-bench-m --zone=europe-west1-b
  gsutil cp gs://pfe-fraud-dataproc-bucket/code/bench_ray.py ~/
  sudo /opt/conda/default/bin/python ~/bench_ray.py

⚠ Le head Ray tourne en root (init-action) → exécuter en root.
  Ne PAS utiliser JupyterLab (conflit de permissions /tmp/ray).
"""
import os, time, json
os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = "30"

import ray
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import average_precision_score, roc_auc_score
import gcsfs
import mlflow

# ─────────────────── CONFIG ───────────────────
BUCKET        = "pfe-fraud-dataproc-bucket"
TRAIN_PATH    = f"gs://{BUCKET}/features/fraud_train_proc.parquet"
TEST_PATH     = f"gs://{BUCKET}/features/fraud_test_proc.parquet"
LABEL         = "isFraud"
NUM_WORKERS   = 5
CPUS_PER_WORKER = 2
NUM_TREES     = 500

MLFLOW_URI    = "http://34.140.133.17:5000"
EXPERIMENT    = "ieee_fraud_lgbm"

LGB_PARAMS = {
    "objective": "binary",
    "learning_rate": 0.03,
    "num_leaves": 512,
    "min_data_in_leaf": 80,
    "feature_fraction": 0.5,
    "bagging_fraction": 0.7,
    "bagging_freq": 1,
    "lambda_l1": 0.1,
    "lambda_l2": 0.1,
    "is_unbalance": True,
    "num_threads": CPUS_PER_WORKER,
    "verbose": -1,
}

# ─────────────────── RAY INIT ───────────────────
# Se connecte au head Ray (lancé par init-action sur le master).
# address="ray-bench-m:6379" pour cluster Dataproc.
ray.init(address="ray-bench-m:6379")
print(f">>> Ray cluster: {ray.cluster_resources()}")

# ─────────────────── DATA ───────────────────
fs = gcsfs.GCSFileSystem()
train_df = pd.read_parquet(f"gs://{BUCKET}/features/fraud_train_proc.parquet", filesystem=fs)
test_df  = pd.read_parquet(f"gs://{BUCKET}/features/fraud_test_proc.parquet", filesystem=fs)

n_train = len(train_df)
n_test  = len(test_df)
feature_cols = [c for c in train_df.columns if c != LABEL]
n_features = len(feature_cols)
print(f">>> Train: {n_train}, Test: {n_test}, Features: {n_features}")

X_train = train_df[feature_cols]
y_train = train_df[LABEL]
X_test  = test_df[feature_cols]
y_test  = test_df[LABEL]

# ─────────────────── RAY TRAIN ───────────────────
from ray.train.lightgbm import LightGBMTrainer
from ray.train import ScalingConfig, RunConfig
import ray.data

# Construire le Ray Dataset pour le sharding automatique
train_ray = ray.data.from_pandas(train_df)

def train_func(config):
    """Fonction d'entraînement exécutée sur chaque worker."""
    import ray.train
    shard = ray.train.get_dataset_shard("train")
    shard_df = shard.materialize().to_pandas()

    X = shard_df.drop(columns=[LABEL])
    y = shard_df[LABEL]

    dtrain = lgb.Dataset(X, label=y, free_raw_data=False)
    bst = lgb.train(config["lgb_params"], dtrain, num_boost_round=config["num_trees"])

    # Sauvegarder le modèle sur GCS (pas /tmp — workers sur des VMs différentes)
    model_path = f"gs://{BUCKET}/ray_results/lgb_model_ray.txt"
    fs = gcsfs.GCSFileSystem()
    with fs.open(model_path, "w") as f:
        f.write(bst.model_to_string())

scaling = ScalingConfig(
    num_workers=NUM_WORKERS,
    resources_per_worker={"CPU": CPUS_PER_WORKER},
    use_gpu=False,
)

trainer = LightGBMTrainer(
    train_loop_config={"lgb_params": LGB_PARAMS, "num_trees": NUM_TREES},
    scaling_config=scaling,
    run_config=RunConfig(storage_path=f"gs://{BUCKET}/ray_results"),
    datasets={"train": train_ray},
    train_loop_per_worker=train_func,
)

print(">>> Fit LightGBM (Ray Train)...")
t0 = time.time()
result = trainer.fit()
fit_time = time.time() - t0
print(f">>> Fit terminé en {fit_time:.1f}s")

# ─────────────────── EVAL ───────────────────
model_path = f"gs://{BUCKET}/ray_results/lgb_model_ray.txt"
with fs.open(model_path, "r") as f:
    bst = lgb.Booster(model_str=f.read())

y_prob = bst.predict(X_test)
auc_pr  = average_precision_score(y_test, y_prob)
auc_roc = roc_auc_score(y_test, y_prob)
print(f">>> AUC-PR  = {auc_pr:.4f}")
print(f">>> AUC-ROC = {auc_roc:.4f}")

# Sauvegarder le temps sur GCS (Ray Train result.metrics retourne None sans checkpoint)
result_data = {"fit_time_s": round(fit_time, 2), "auc_pr": round(auc_pr, 4), "auc_roc": round(auc_roc, 4)}
with fs.open(f"gs://{BUCKET}/ray_results/bench_ray_result.json", "w") as f:
    json.dump(result_data, f)

# ─────────────────── MLFLOW ───────────────────
try:
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name=f"ray_{NUM_WORKERS}w_{CPUS_PER_WORKER}cpu_{NUM_TREES}trees"):
        mlflow.set_tags({
            "benchmark": "spark_vs_ray_fair",
            "engine": "ray_train",
            "num_workers": str(NUM_WORKERS),
            "num_threads": str(CPUS_PER_WORKER),
        })
        mlflow.log_params({
            "num_iterations": NUM_TREES,
            "num_workers": NUM_WORKERS,
            "num_threads": CPUS_PER_WORKER,
            "n_features": n_features,
            "n_train": n_train,
            "n_test": n_test,
        })
        mlflow.log_metrics({
            "time_train_lgbm_s": round(fit_time, 2),
            "test_auc_pr": round(auc_pr, 4),
            "test_auc_roc": round(auc_roc, 4),
        })
    print(">>> MLflow loggé OK.")
except Exception as e:
    print(f"⚠ MLflow log échoué : {e}")
    print(f"   Résultats : fit={fit_time:.1f}s, AUC-PR={auc_pr:.4f}, AUC-ROC={auc_roc:.4f}")

ray.shutdown()
