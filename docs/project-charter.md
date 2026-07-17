# Cadrage du projet

## Problématique métier

Quels indicateurs tactiques et statistiques permettent de distinguer les équipes performantes pendant la Coupe du monde 2022 ?

## Utilisateurs cibles

- Analystes sportifs
- Entraîneurs et staffs techniques
- Journalistes sportifs
- Supporters souhaitant explorer les données

## Questions analytiques

1. Quelles équipes produisent les meilleures occasions ?
2. Quelles équipes convertissent le mieux leurs occasions en buts ?
3. Quels joueurs contribuent le plus offensivement par 90 minutes ?
4. Existe-t-il une relation entre possession, tirs, xG et victoire ?
5. Quelles zones du terrain concentrent les tirs et les récupérations ?

## Périmètre initial

- Compétition : Coupe du monde masculine 2022
- Sources : compétitions, matchs, compositions et événements StatsBomb
- Traitement : batch
- Format Lakehouse : Delta Lake
- Couches : Bronze, Silver et Gold
- Restitution : Power BI Desktop

## Hors périmètre initial

- Données en temps réel
- Application web publique
- Paris sportifs
- Reconnaissance vidéo
- Modèle de machine learning en production

## Critères de réussite

- Pipeline relançable sans créer de doublons
- Données invalides identifiées et mises en quarantaine
- Modèle Gold documenté et exploitable dans Power BI
- Dashboard répondant aux questions métier
- Dépôt reproductible et compréhensible par un recruteur
