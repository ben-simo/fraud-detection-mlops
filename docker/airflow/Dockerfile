# Image Airflow custom : providers Google + libs nécessaires
# Vérifier la version core avant de builder :
#   docker exec airflow airflow version
ARG AIRFLOW_VERSION=2.9.3
FROM apache/airflow:${AIRFLOW_VERSION}

RUN pip install --no-cache-dir \
    "apache-airflow-providers-google==10.17.0" \
    "google-cloud-storage" \
    "requests"
