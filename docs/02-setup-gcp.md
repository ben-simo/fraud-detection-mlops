# Setup GCP

## Prérequis

- Compte GCP avec facturation activée
- `gcloud` CLI installé et authentifié (`gcloud auth login`)
- Quotas suffisants dans `europe-west1` :
  - CPU : au moins 24 vCPUs (cluster 1+5 nodes × 4 vCPUs)
  - Hyperdisk Balanced : 500 Go max (6 disques × 50 Go = 300 Go par cluster)

## Étapes

Exécuter les scripts dans l'ordre **depuis Cloud Shell** :

```bash
# 1. Activer les APIs
bash infrastructure/gcp/01_project_apis.sh

# 2. Créer le service account
bash infrastructure/gcp/02_service_account.sh

# 3. Créer le bucket GCS
bash infrastructure/gcp/03_gcs_bucket.sh

# 4. Créer l'Artifact Registry
bash infrastructure/gcp/04_artifact_registry.sh

# 5. Configurer le firewall
bash infrastructure/gcp/05_firewall.sh

# 6. Créer la VM
bash infrastructure/gcp/06_create_vm.sh

# 7. Réserver l'IP statique (CRITIQUE — faire avant tout stop/start)
bash infrastructure/gcp/07_static_ip.sh
```

## Points d'attention

### IP statique
L'IP doit être réservée AVANT le premier `stop/start` de la VM. Sinon GCP attribue une nouvelle IP et toutes les références (Kafka, MLflow, producer) cassent.

### Service Account
Le SA `fraud-pipeline-sa` n'a **pas** les rôles Compute Engine. C'est voulu : les opérations Compute (firewall, SCP, describe) se font depuis Cloud Shell avec les credentials owner. Le SA sert uniquement pour Dataproc, GCS et Artifact Registry.

### Scopes VM
La VM doit être créée avec `--scopes=cloud-platform` pour que Dataproc/GCS fonctionnent depuis les containers.

### Quotas Hyperdisk
Chaque cluster Dataproc (1 master + 5 workers × 50 Go) consomme 300 Go sur un quota de 500 Go. Ne jamais avoir deux clusters en parallèle.

## Pousser les init-actions

Après le setup GCP, pousser les init-actions sur GCS :

```bash
gsutil cp infrastructure/init-actions/pip-install.sh gs://pfe-fraud-dataproc-bucket/init-actions/
gsutil cp infrastructure/init-actions/ray-init.sh gs://pfe-fraud-dataproc-bucket/init-actions/
```
