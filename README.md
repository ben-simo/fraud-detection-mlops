# Détection de Fraude Financière en Temps Réel — Pipeline MLOps Distribué

**Projet de Fin d'Études** — Université Ibn Zohr, Faculté Polydisciplinaire de Ouarzazate  
**Auteur** : Mohamed Bencharif  
**Encadrement** : Département Informatique, FPO

---

## Vue d'ensemble

Ce dépôt contient l'intégralité du code, de la configuration et de la documentation nécessaires pour reproduire un pipeline end-to-end de détection de fraude financière en temps réel, articulé autour de trois axes :

1. **Étude comparative ML centralisée** — 7 familles de modèles (Random Forest, XGBoost, LightGBM × 3 stratégies de rééquilibrage, DNN, TabTransformer, FT-Transformer, GraphSAGE, LightGBM+Optuna) évalués sur le dataset IEEE-CIS Fraud Detection (590 540 transactions, 464 features).

2. **Benchmark distribué Spark vs Ray** — Entraînement LightGBM distribué sur GCP Dataproc : Spark/SynapseML (5 workers) vs Ray Train (5 workers) vs Ray mono-nœud, mêmes hyperparamètres, même parquet préprocessé, seul le `fit` est chronométré.

3. **Pipeline temps réel Kappa** — Architecture `Producer → Kafka → fraud-api (FastAPI) → Elasticsearch → Kibana` avec réentraînement automatique champion/challenger orchestré par Airflow toutes les 29 527 transactions.

### Résultats clés

| Métrique | Valeur |
|----------|--------|
| Meilleur modèle centralisé | LightGBM baseline (AUC-PR: 0.5839, AUC-ROC: 0.9138) |
| Spark 5 workers (fit) | 866.0 s — AUC-PR: 0.6508 |
| Ray 5 workers (fit) | 334.8 s (×2.59 plus rapide) — AUC-PR: 0.6455 |
| Ray mono-nœud (fit) | 219.6 s (×3.94) — AUC-PR: 0.6459 |
| Latence serving (p99) | 2.55 ms |
| Gain AUC-PR après 4 retrains | 0.5142 → 0.6001 (+16.7%) |
| Coût estimé 4 retrains Dataproc | ~$1.74 |

---

## Architecture

```
                    ┌─────────────────────────────────────────────────┐
                    │              GCP (europe-west1)                 │
                    │                                                 │
  ┌──────────┐     │  ┌──────────┐    ┌───────────┐   ┌──────────┐  │
  │ Producer │────▶│  │  Kafka   │───▶│ fraud-api │──▶│   ES     │  │
  │ (Python) │     │  │ (KRaft)  │    │ (FastAPI) │   │ 8.13.0   │  │
  └──────────┘     │  └──────────┘    └─────┬─────┘   └────┬─────┘  │
                    │                       │              │         │
                    │              ┌────────▼────────┐     │         │
                    │              │    Airflow       │     │         │
                    │              │  (DAG retrain)   │     │         │
                    │              └────────┬────────┘     │         │
                    │                       │              │         │
                    │              ┌────────▼────────┐  ┌──▼──────┐  │
                    │              │    Dataproc      │  │ Kibana  │  │
                    │              │  (SynapseML/Ray) │  │ 8.13.0  │  │
                    │              └─────────────────┘  └─────────┘  │
                    │                                                 │
                    │  ┌──────────┐    ┌────────────┐                │
                    │  │  MLflow  │    │ Metricbeat │                │
                    │  │  2.16.2  │    │   8.13.0   │                │
                    │  └──────────┘    └────────────┘                │
                    └─────────────────────────────────────────────────┘
```

**5 étapes canoniques (Kappa)** : Source de données → Journal immuable (Kafka) → Stream processing (fraud-api) → Vues matérialisées (Elasticsearch) → Tableau de bord (Kibana)

---

## Prérequis

- Compte GCP avec facturation activée
- `gcloud` CLI installé et configuré
- Docker & Docker Compose
- Python 3.11+
- Compte Kaggle (pour le dataset IEEE-CIS)

---

## Démarrage rapide

### 1. Cloner et configurer

```bash
git clone https://github.com/ben-simo/fraud-detection-mlops.git
cd fraud-detection-mlops
cp .env.example .env
nano .env  # Remplir les variables (projet GCP, IP, etc.)
```

### 2. Infrastructure GCP

```bash
# Depuis Cloud Shell (pas depuis la VM)
bash infrastructure/gcp/01_project_apis.sh
bash infrastructure/gcp/02_service_account.sh
bash infrastructure/gcp/03_gcs_bucket.sh
bash infrastructure/gcp/04_artifact_registry.sh
bash infrastructure/gcp/05_firewall.sh
bash infrastructure/gcp/06_create_vm.sh
bash infrastructure/gcp/07_static_ip.sh
```

### 3. Déployer la stack Docker (sur la VM)

```bash
# SSH sur la VM
gcloud compute ssh mlflow-server --zone=europe-west1-b

# Cloner le repo
git clone https://github.com/ben-simo/fraud-detection-mlops.git
cd fraud-detection-mlops
cp .env.example .env && nano .env

# Préparer les permissions Airflow
mkdir -p airflow/state airflow/logs
sudo chown -R 50000:0 airflow/state airflow/logs

# Lancer
docker compose up -d

# Vérifier (attendre ~2 min pour ES)
bash ops/check_health.sh
```

### 4. Pousser les scripts sur GCS

```bash
# Init-actions (nécessaires pour créer les clusters Dataproc)
gsutil cp infrastructure/init-actions/pip-install.sh gs://pfe-fraud-dataproc-bucket/init-actions/
gsutil cp infrastructure/init-actions/ray-init.sh gs://pfe-fraud-dataproc-bucket/init-actions/

# Scripts de benchmark
gsutil cp scripts/bench_spark.py gs://pfe-fraud-dataproc-bucket/code/
gsutil cp scripts/bench_ray.py gs://pfe-fraud-dataproc-bucket/code/
```

### 5. Reproduire les expériences

Voir [docs/04-running-experiments.md](docs/04-running-experiments.md) pour les instructions détaillées.

---

## Structure du dépôt

```
fraud-detection-mlops/
├── README.md                              ← Ce fichier
├── docker-compose.yml                     ← Stack 7 services
├── .env.example                           ← Variables d'environnement (template)
├── .gitignore
│
├── docker/                                ← Dockerfiles & configs
│   ├── airflow/Dockerfile
│   ├── fraud-api/
│   │   ├── Dockerfile
│   │   ├── fraud_api.py                   ← ⚠ PLACEHOLDER (voir ci-dessous)
│   │   └── requirements.txt
│   ├── mlflow/Dockerfile
│   └── metricbeat/metricbeat.yml
│
├── infrastructure/                        ← Provisioning cloud
│   ├── gcp/                               ← Scripts setup GCP (01→07)
│   ├── dataproc/                          ← Création clusters Dataproc
│   │   ├── create_spark_cluster.sh
│   │   ├── create_ray_cluster.sh
│   │   └── create_retrain_cluster.sh
│   └── init-actions/                      ← Scripts d'initialisation Dataproc
│       ├── pip-install.sh
│       └── ray-init.sh
│
├── airflow/dags/
│   └── fraud_retrain_dag.py               ← DAG champion/challenger
│
├── scripts/                               ← Benchmark distribué
│   ├── bench_spark.py                     ← Spark + SynapseML
│   └── bench_ray.py                       ← Ray Train
│
├── streaming/
│   └── producer_kafka.py                  ← Simulateur de flux Kafka
│
├── notebooks/                             ← ⚠ À copier depuis Kaggle
│   └── (voir docs/04-running-experiments.md)
│
├── ops/                                   ← Scripts opérationnels
│   ├── check_health.sh                    ← Diagnostic santé
│   ├── start_simulation.sh                ← Reset + lancement simulation
│   └── reset_state.sh                     ← Remise à zéro (entre simulations)
│
├── kibana/                                ← Dashboards exportés (NDJSON)
├── docs/                                  ← Documentation détaillée
│   ├── 01-architecture.md
│   ├── 02-setup-gcp.md
│   ├── 03-deployment-docker.md
│   ├── 04-running-experiments.md
│   └── 05-troubleshooting.md
│
└── results/                               ← Résultats MLflow exportés
```

### ⚠ Fichiers à récupérer manuellement

Certains fichiers ne sont pas dans ce repo (trop volumineux ou contenant des données sensibles) :

| Fichier | Emplacement | Comment le récupérer |
|---------|-------------|---------------------|
| `fraud_api.py` (réel) | VM `~/mlflow/fraud-api/` | `gcloud compute scp mlflow-server:~/mlflow/fraud-api/fraud_api.py docker/fraud-api/` |
| `pipeline_retrain.py` | GCS `code/` | `gsutil cp gs://pfe-fraud-dataproc-bucket/code/pipeline_retrain.py scripts/` |
| Notebooks centralisés | Kaggle | Exporter depuis Kaggle → `notebooks/` |
| Dataset IEEE-CIS | Kaggle | `kaggle competitions download -c ieee-fraud-detection` |
| Dashboards Kibana | Kibana UI | Stack Management → Saved Objects → Export |
| `lgb_model.txt` (champion) | GCS `models/` | `gsutil cp gs://pfe-fraud-dataproc-bucket/models/lgb_model_v*.txt models/` |

---

## Versions des outils

| Outil | Version | Rôle |
|-------|---------|------|
| Python | 3.11.x | Langage principal |
| LightGBM | 4.3.0 | Algorithme de gradient boosting |
| SynapseML | 1.0.5 | LightGBM distribué sur Spark |
| Apache Spark | 3.5.x | Moteur de traitement distribué |
| Ray | 2.55.1 | Framework distribué alternatif |
| MLflow | 2.16.2 | Suivi des expériences |
| Apache Kafka | 7.6.1 (cp-kafka) | Journal de messages immuable |
| Apache Airflow | 2.9.3 | Orchestration du DAG |
| Elasticsearch | 8.13.0 | Indexation des prédictions |
| Kibana | 8.13.0 | Tableaux de bord temps réel |
| Metricbeat | 8.13.0 | Métriques d'infrastructure |
| FastAPI | 0.111.0 | Service de prédiction en ligne |
| Docker Compose | v2 | Gestion des conteneurs |

---

## Dataset

**IEEE-CIS Fraud Detection** (Kaggle)
- 590 540 transactions, 434 colonnes brutes → 464 features après preprocessing
- Split temporel : 60/20/20 (entraînement centralisé) ou 80/20 (benchmark distribué)
- UID client : `card1 + addr1 + D1_normalized`
- Classe minoritaire (fraude) : ~3.5%

---

## Licence

MIT — voir [LICENSE](LICENSE)

---

## Contact

Mohamed Bencharif — Université Ibn Zohr, FPO Ouarzazate  
GitHub : [@ben-simo](https://github.com/ben-simo)
