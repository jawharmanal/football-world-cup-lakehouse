# World Cup 2022 Analytics Lakehouse

Projet personnel de data engineering consacré à l'analyse des données événementielles de la Coupe du monde 2022.

## Objectif

Construire une plateforme de données de bout en bout avec Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, Delta Lake et Power BI.

## Architecture

1. Ingestion des fichiers JSON StatsBomb avec Azure Data Factory.
2. Conservation des données sources dans la couche Bronze.
3. Nettoyage et normalisation avec PySpark dans la couche Silver.
4. Création d'un modèle dimensionnel et de KPI dans la couche Gold.
5. Visualisation des performances des équipes et joueurs avec Power BI.

## Source des données

Les données proviennent de [StatsBomb Open Data](https://github.com/statsbomb/open-data). Toute publication issue de ces données doit attribuer clairement StatsBomb comme source.

## État du projet

- [x] Cadrage initial
- [x] Activation et sécurisation de l'abonnement Azure for Students
- [x] Validation du dataset et identification de la compétition 43 / saison 106
- [ ] Création des ressources Azure
- [ ] Pipeline d'ingestion Bronze
- [ ] Transformations Silver
- [ ] Modèle Gold
- [ ] Rapport Power BI
- [ ] Tests, documentation et CI/CD

## Sécurité

Les secrets, clés, fichiers de configuration personnels et exports de données volumineux ne doivent jamais être versionnés dans Git.
