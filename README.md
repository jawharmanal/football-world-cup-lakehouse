# World Cup 2022 Analytics Lakehouse

Projet personnel de data engineering consacré à l’analyse de la Coupe du monde FIFA 2022 à partir des données ouvertes de StatsBomb.

## Objectif

Construire une chaîne de données de bout en bout : ingestion, stockage, transformations Lakehouse en architecture médaillon et restitution décisionnelle.

## Architecture

```text
StatsBomb Open Data
        |
        v
Azure Data Factory
        |
        v
ADLS Gen2 (landing)
        |
        v
Databricks + Delta Lake
 Bronze -> Silver -> Gold
        |
        v
Power BI
```

## Technologies

- Azure Data Factory pour l’orchestration et l’ingestion
- Azure Data Lake Storage Gen2 pour le stockage cloud
- Databricks, PySpark et Delta Lake pour les transformations
- Architecture Lakehouse Bronze, Silver et Gold
- Power BI et DAX pour la visualisation
- GitHub pour le versionnement

## Données

- Compétition : FIFA World Cup 2022
- Identifiants StatsBomb : compétition `43`, saison `106`
- 64 matchs et 32 équipes
- 234 637 événements en couche Bronze
- 3 244 lignes de compositions et 829 joueurs uniques en couche Silver

Les données proviennent de [StatsBomb Open Data](https://github.com/statsbomb/open-data). StatsBomb doit être cité comme source lors de leur utilisation ou publication.

## Couches du Lakehouse

- **Bronze** : conservation des données JSON brutes (compétitions, matchs, événements et compositions).
- **Silver** : nettoyage, déduplication, normalisation des types et création des tables matchs, événements, joueurs et équipes.
- **Gold** : agrégats prêts pour l’analyse des équipes, des joueurs et des matchs.

## Résultats Power BI

Le rapport contient :

- 64 matchs
- 172 buts
- 32 équipes
- 681 joueurs analysés
- classement des équipes par buts
- classement des buteurs
- analyse chronologique des matchs
- détail des scores, phases et expected goals (xG)

Le rapport et les exports Gold sont disponibles dans [`powerbi/`](powerbi/).

## État du projet

- [x] Cadrage et validation du dataset
- [x] Infrastructure Azure et ADLS Gen2
- [x] Pipelines d’ingestion Azure Data Factory
- [x] Couches Bronze, Silver et Gold avec PySpark/Delta Lake
- [x] Modèle analytique et contrôles de qualité
- [x] Dashboard Power BI
- [x] Documentation du projet

## Sécurité et maîtrise des coûts

Les secrets, clés et fichiers de configuration personnels ne sont pas versionnés. Le projet utilise Azure for Students et Databricks Free Edition afin de limiter les coûts.
