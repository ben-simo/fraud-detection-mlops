# Dashboards Kibana

## Exporter depuis Kibana

1. Ouvrir Kibana : http://34.140.133.17:5601
2. Stack Management → Saved Objects
3. Sélectionner les dashboards → Export
4. Sauvegarder le fichier NDJSON ici

## Importer

1. Stack Management → Saved Objects → Import
2. Sélectionner `dashboards.ndjson`

## Points d'attention

- `typeMigrationVersion` doit être `8.5.0` (pas `8.8.0`) pour Kibana 8.13
- Après import, vérifier les data views et rafraîchir les field lists
- Le time picker par défaut est "Last 15 minutes" — le changer pour la durée de la simulation
- Les runtime fields (`model_label`) doivent être ajoutés aux deux data views si elles coexistent
- Modifier un dashboard importé : utiliser "Save as" (pas "Save" direct — erreur Bad Request)
