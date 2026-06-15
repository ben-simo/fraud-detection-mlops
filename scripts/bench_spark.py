#!/usr/bin/env python3
"""
BENCHMARK SPARK — SynapseML LightGBM distribué (5 workers x 2 threads)

Comparaison équitable vs Ray : MÊME parquet préprocessé, MÊMES hyperparamètres,
MÊME nb workers (5), MÊME threads/worker (2), on chronomètre SEUL le fit.

SUBMIT (depuis Cloud Shell) :
  gcloud dataproc jobs submit pyspark gs://pfe-fraud-dataproc-bucket/code/bench_spark.py \
    --cluster=fraud-spark-ray --region=europe-west1 \
    --properties="spark.jars.packages=com.microsoft.azure:synapseml_2.12:1.0.5,\
spark.dynamicAllocation.enabled=false,\
spark.executor.instances=6,\
spark.executor.cores=2,\
spark.task.cpus=2"

⚠ NE PAS interrompre après la ligne AUC — le script enchaîne sur le log MLflow.
  Attendre ">>> MLflow loggé OK."
"""
import os, time
os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = "30"

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from synapse.ml.lightgbm import LightGBMClassifier
import mlflow

# ─────────────────── CONFIG ───────────────────
BUCKET       = "pfe-fraud-dataproc-bucket"
TRAIN_PATH   = f"gs://{BUCKET}/features/fraud_train_proc.parquet"
TEST_PATH    = f"gs://{BUCKET}/features/fraud_test_proc.parquet"
LABEL        = "isFraud"
NUM_WORKERS  = 5
NUM_THREADS  = 2
NUM_ITERS    = 500

MLFLOW_URI   = "http://34.140.133.17:5000"
EXPERIMENT   = "ieee_fraud_lgbm"

LGB_PARAMS = dict(
    objective="binary", learningRate=0.03, numLeaves=512,
    minDataInLeaf=80, featureFraction=0.5,
    baggingFraction=0.7, baggingFreq=1,
    lambdaL1=0.1, lambdaL2=0.1,
)

# ─────────────────── SPARK ───────────────────
spark = SparkSession.builder.appName("bench_spark_fair").getOrCreate()

train_df = spark.read.parquet(TRAIN_PATH)
test_df  = spark.read.parquet(TEST_PATH)

# Pré-calculer les counts AVANT le fit (évite un job Spark après)
n_train = train_df.count()
n_test  = test_df.count()
print(f">>> Train: {n_train} rows, Test: {n_test} rows")

feature_cols = [c for c in train_df.columns if c != LABEL]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
train_vec = assembler.transform(train_df).select("features", LABEL)
test_vec  = assembler.transform(test_df).select("features", LABEL)

lgbm = LightGBMClassifier(
    featuresCol="features", labelCol=LABEL,
    numTasks=NUM_WORKERS, numThreadPerTask=NUM_THREADS,
    numIterations=NUM_ITERS,
    useBarrierExecutionMode=True,
    isUnbalance=True,
    **LGB_PARAMS
)

# ─────────────────── FIT (chronomètre) ───────────────────
print(">>> Fit LightGBM (SynapseML)...")
t0 = time.time()
model = lgbm.fit(train_vec)
fit_time = time.time() - t0
print(f">>> Fit terminé en {fit_time:.1f}s")

# ─────────────────── EVAL ───────────────────
preds = model.transform(test_vec)
evaluator_pr  = BinaryClassificationEvaluator(labelCol=LABEL, metricName="areaUnderPR")
evaluator_roc = BinaryClassificationEvaluator(labelCol=LABEL, metricName="areaUnderROC")
auc_pr  = evaluator_pr.evaluate(preds)
auc_roc = evaluator_roc.evaluate(preds)
print(f">>> AUC-PR  = {auc_pr:.4f}")
print(f">>> AUC-ROC = {auc_roc:.4f}")

# ─────────────────── MLFLOW ───────────────────
try:
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name=f"spark_5w_{NUM_THREADS}t_{NUM_ITERS}trees"):
        mlflow.set_tags({
            "benchmark": "spark_vs_ray_fair",
            "engine": "spark_synapseml",
            "num_workers": str(NUM_WORKERS),
            "num_threads": str(NUM_THREADS),
        })
        mlflow.log_params({
            "num_iterations": NUM_ITERS,
            "num_workers": NUM_WORKERS,
            "num_threads": NUM_THREADS,
            "n_features": len(feature_cols),
            "n_train": n_train,
            "n_test": n_test,
            **{k: str(v) for k, v in LGB_PARAMS.items()},
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

spark.stop()
