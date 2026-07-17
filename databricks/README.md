# Databricks Lakehouse

Les notebooks PySpark ont été développés et exécutés avec Databricks Free Edition en mode Serverless.

## Notebooks

1. [`bronze/01_bronze_ingestion.py`](bronze/01_bronze_ingestion.py) : ingestion des données StatsBomb et création des tables Delta Bronze.
2. [`silver/02_silver_transformations.py`](silver/02_silver_transformations.py) : nettoyage, typage, déduplication et normalisation des matchs, événements, compositions et équipes.
3. [`gold/03_gold_analytics.py`](gold/03_gold_analytics.py) : création des agrégats analytiques par équipe, joueur et match, puis export pour Power BI.

## Tables principales

```text
Bronze
├── bronze_competitions
├── bronze_matches
├── bronze_events
└── bronze_lineups

Silver
├── silver_matches
├── silver_events
├── silver_lineups
└── silver_teams

Gold
├── gold_team_summary
├── gold_player_summary
└── gold_match_summary
```

Les notebooks Azure Databricks et Databricks Free Edition utilisent les mêmes concepts PySpark, Delta Lake et architecture médaillon. Free Edition a été retenue pour l’exécution afin d’éviter les coûts et les contraintes de quota CPU du compte étudiant Azure.
