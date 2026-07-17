# Dashboard Power BI

## Fichiers

- `football-world-cup-dashboard.pbix` : rapport Power BI Desktop
- `gold_team_summary.csv` : indicateurs par équipe
- `gold_player_summary.csv` : indicateurs par joueur
- `gold_match_summary.csv` : détail et indicateurs par match

## Modèle

Relation active :

```text
gold_team_summary[team_id] (1) -> (*) gold_player_summary[team_id]
```

`gold_match_summary` reste indépendante, car chaque match possède une équipe à domicile et une équipe à l’extérieur.

## Mesures DAX principales

```DAX
Total Matchs = COUNTROWS(gold_match_summary)

Total Buts =
    SUM(gold_match_summary[home_score])
    + SUM(gold_match_summary[away_score])

Total Équipes = COUNTROWS(gold_team_summary)

Total Joueurs = COUNTROWS(gold_player_summary)
```

## Pages du rapport

1. **Vue d’ensemble** : KPI, buts par équipe et meilleurs buteurs.
2. **Analyse des matchs** : évolution chronologique, filtre par phase et détail des matchs.
