"""
fraud_api.py — Service de scoring temps réel (FastAPI + LightGBM)

Architecture :
  - Thread consommateur Kafka : lit les transactions du topic, score chacune
    via LightGBM, indexe le résultat dans Elasticsearch.
  - Endpoints REST : /health, /model-info, /stats, /predict, /reload
  - Auto-trigger : toutes les BATCH_PER_CYCLE transactions, envoie un POST
    à l'API REST Airflow pour déclencher le DAG fraud_retrain.

⚠ PLACEHOLDER — Remplacer par le vrai fraud_api.py déployé sur la VM.
  Le fichier réel se trouve dans ~/mlflow/fraud-api/fraud_api.py sur la VM.
  Copier avec :
    gcloud compute scp mlflow-server:~/mlflow/fraud-api/fraud_api.py \
        docker/fraud-api/fraud_api.py --zone=europe-west1-b
"""

import os
from fastapi import FastAPI

app = FastAPI(title="Fraud Detection API")

@app.get("/health")
def health():
    return {"status": "ok", "message": "PLACEHOLDER — remplacer par le vrai code"}

# Voir le README pour les instructions de récupération du code réel.
