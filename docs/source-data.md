# Données sources

## Dataset sélectionné

- Fournisseur : StatsBomb Open Data
- Compétition : FIFA World Cup
- Saison : 2022
- `competition_id` : `43`
- `season_id` : `106`
- Format : JSON

## Objets utilisés

| Objet | Rôle |
|---|---|
| `competitions.json` | Métadonnées des compétitions et saisons |
| `matches/43/106.json` | Liste et métadonnées des matchs de 2022 |
| `lineups/{match_id}.json` | Compositions et joueurs d'un match |
| `events/{match_id}.json` | Événements détaillés d'un match |
| `three-sixty/{match_id}.json` | Données 360 lorsqu'elles sont disponibles |

## Attribution

StatsBomb doit être mentionné comme source lors de toute publication, analyse ou visualisation reposant sur ces données.

## Validation locale

Depuis la racine du projet :

```powershell
python scripts/explore_source.py
```

Les fichiers sont enregistrés dans `data/source/` et ne sont pas versionnés par Git.

## Résultat de validation

Validation exécutée le 17 juillet 2026 :

| Contrôle | Résultat |
|---|---:|
| Compétition | FIFA World Cup |
| Saison | 2022 |
| Matchs | 64 |
| Équipes | 32 |
| Identifiants de match uniques | 64 |

Le nombre d'identifiants uniques étant égal au nombre de matchs, aucun doublon de match n'a été détecté dans les métadonnées sources.
