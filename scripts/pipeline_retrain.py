"""
pipeline_retrain.py — Script PySpark soumis par le DAG Airflow.

Ce script est exécuté sur un cluster Dataproc lors de chaque cycle de réentraînement.
Il :
  1. Récupère les transactions accumulées dans Elasticsearch (fenêtre cumulative)
  2. Entraîne un challenger LightGBM via SynapseML
  3. Évalue le challenger contre le champion courant
  4. Écrit result.json sur GCS (contenant la décision de promotion)
  5. Si meilleur : met à jour current_champion.json et sauvegarde le modèle

Arguments :
  --cycle N    Numéro du cycle de réentraînement (1, 2, 3, 4)

⚠ PLACEHOLDER — Remplacer par le vrai pipeline_retrain.py.
  Le fichier réel est sur GCS :
    gsutil cp gs://pfe-fraud-dataproc-bucket/code/pipeline_retrain.py scripts/
"""
print("⚠ PLACEHOLDER — remplacer par le vrai pipeline_retrain.py")
