# Infrastructure Azure

## Abonnement et gouvernance

- Offre : Azure for Students
- Environnement : `dev`
- Resource Group : `rg-wc2022-lakehouse-dev`
- Région principale : France Central
- Budget mensuel d'alerte : 15 USD

## Azure Data Lake Storage Gen2

- Compte : `stwc2022jawhar`
- Type : StorageV2 / ADLS Gen2
- Performance : Standard
- Redondance : LRS
- Niveau d'accès : Hot
- Espace de noms hiérarchique : activé
- Accès anonyme : désactivé

### Conteneurs

| Conteneur | Usage | Accès anonyme |
|---|---|---|
| `landing` | Zone d'arrivée des fichiers sources | Privé |
| `bronze` | Données brutes historisées | Privé |
| `silver` | Données nettoyées et validées | Privé |
| `gold` | Modèle métier pour Power BI | Privé |
| `quarantine` | Données rejetées par les contrôles | Privé |

Le conteneur système `$logs`, créé automatiquement par Azure, ne fait pas partie des couches fonctionnelles du Lakehouse.

## Sécurité

- Aucun secret ni clé de stockage n'est versionné dans Git.
- Les services Azure utiliseront des identités managées et Azure RBAC lorsque cela est possible.
- Les conteneurs restent privés.

## Azure Data Factory

- Fabrique : `adf-wc2022-jawhar-dev`
- Version : V2
- Resource Group : `rg-wc2022-lakehouse-dev`
- Région : France Central
- Identité : identité managée affectée par le système

### Autorisation ADLS

L'identité managée `adf-wc2022-jawhar-dev` possède le rôle Azure RBAC `Storage Blob Data Contributor` sur le compte `stwc2022jawhar`. Cette autorisation permet à ADF de lire et écrire les données sans stocker de clé de compte dans les linked services ou dans Git.
