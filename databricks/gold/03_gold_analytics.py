# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "football_wc"

silver_matches = spark.table(f"{CATALOG}.{SCHEMA}.silver_matches")
silver_events = spark.table(f"{CATALOG}.{SCHEMA}.silver_events")
silver_lineups = spark.table(f"{CATALOG}.{SCHEMA}.silver_lineups")
silver_teams = spark.table(f"{CATALOG}.{SCHEMA}.silver_teams")

print("Tables Silver chargées")
print("Matchs :", silver_matches.count())
print("Événements :", silver_events.count())
print("Compositions :", silver_lineups.count())
print("Équipes :", silver_teams.count())

# COMMAND ----------

home_results = silver_matches.select(
    F.col("match_id"),
    F.col("home_team_id").alias("team_id"),
    F.col("home_team_name").alias("team_name"),
    F.col("home_score").alias("goals_for"),
    F.col("away_score").alias("goals_against"),
    F.when(F.col("home_score") > F.col("away_score"), 1).otherwise(0).alias("win"),
    F.when(F.col("home_score") == F.col("away_score"), 1).otherwise(0).alias("draw"),
    F.when(F.col("home_score") < F.col("away_score"), 1).otherwise(0).alias("loss")
)

away_results = silver_matches.select(
    F.col("match_id"),
    F.col("away_team_id").alias("team_id"),
    F.col("away_team_name").alias("team_name"),
    F.col("away_score").alias("goals_for"),
    F.col("home_score").alias("goals_against"),
    F.when(F.col("away_score") > F.col("home_score"), 1).otherwise(0).alias("win"),
    F.when(F.col("away_score") == F.col("home_score"), 1).otherwise(0).alias("draw"),
    F.when(F.col("away_score") < F.col("home_score"), 1).otherwise(0).alias("loss")
)

team_results = home_results.unionByName(away_results)

gold_team_summary = (
    team_results
    .groupBy("team_id", "team_name")
    .agg(
        F.countDistinct("match_id").alias("matches_played"),
        F.sum("win").alias("wins"),
        F.sum("draw").alias("draws"),
        F.sum("loss").alias("losses"),
        F.sum("goals_for").alias("goals_for"),
        F.sum("goals_against").alias("goals_against")
    )
    .withColumn(
        "goal_difference",
        F.col("goals_for") - F.col("goals_against")
    )
    .withColumn(
        "points",
        F.col("wins") * 3 + F.col("draws")
    )
    .withColumn("_created_at", F.current_timestamp())
)

(
    gold_team_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_team_summary")
)

assert gold_team_summary.count() == 32

print("gold_team_summary :", gold_team_summary.count(), "équipes")

display(
    gold_team_summary.orderBy(
        F.desc("points"),
        F.desc("goal_difference"),
        F.desc("goals_for")
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "football_wc"

# Chargement de la table Silver
silver_events = spark.table(
    f"{CATALOG}.{SCHEMA}.silver_events"
)

# Agrégation des statistiques par joueur
gold_player_summary = (
    silver_events
    .filter(F.col("player_id").isNotNull())
    .groupBy(
        "player_id",
        "player_name",
        "team_id",
        "team_name"
    )
    .agg(
        F.countDistinct("match_id").alias("matches_played"),

        F.count("*").alias("total_events"),

        F.sum(
            F.when(
                F.col("event_type_name") == "Pass",
                1
            ).otherwise(0)
        ).alias("passes"),

        F.sum(
            F.when(
                F.col("event_type_name") == "Shot",
                1
            ).otherwise(0)
        ).alias("shots"),

        F.sum(
            F.when(
                (F.col("event_type_name") == "Shot") &
                (F.col("shot_outcome") == "Goal"),
                1
            ).otherwise(0)
        ).alias("goals"),

        F.round(
            F.sum(
                F.coalesce(
                    F.col("shot_xg"),
                    F.lit(0.0)
                )
            ),
            3
        ).alias("total_xg"),

        F.sum(
            F.when(
                F.col("under_pressure") == True,
                1
            ).otherwise(0)
        ).alias("events_under_pressure")
    )
    .withColumn(
        "xg_difference",
        F.round(
            F.col("goals") - F.col("total_xg"),
            3
        )
    )
    .withColumn(
        "_created_at",
        F.current_timestamp()
    )
)

# Enregistrement en table Delta Gold
(
    gold_player_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{CATALOG}.{SCHEMA}.gold_player_summary"
    )
)

# Contrôles qualité
player_count = gold_player_summary.count()
goal_count = gold_player_summary.agg(
    F.sum("goals").alias("total")
).first()["total"]

assert player_count > 0
assert goal_count > 0

print(
    "gold_player_summary :",
    player_count,
    "joueurs"
)

print(
    "Nombre total de buts attribués :",
    goal_count
)

# Affichage des meilleurs buteurs
display(
    gold_player_summary.orderBy(
        F.desc("goals"),
        F.desc("total_xg"),
        F.desc("shots")
    )
)

# COMMAND ----------

F.sum(
    F.when(
        (F.col("event_type_name") == "Shot") &
        (F.col("period") <= 4),
        1
    ).otherwise(0)
).alias("shots"),

F.sum(
    F.when(
        (F.col("event_type_name") == "Shot") &
        (F.col("shot_outcome") == "Goal") &
        (F.col("period") <= 4),
        1
    ).otherwise(0)
).alias("goals"),

F.round(
    F.sum(
        F.when(
            F.col("period") <= 4,
            F.coalesce(F.col("shot_xg"), F.lit(0.0))
        ).otherwise(0.0)
    ),
    3
).alias("total_xg"),

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "football_wc"

silver_events = spark.table(
    f"{CATALOG}.{SCHEMA}.silver_events"
)

gold_player_summary = (
    silver_events
    .filter(F.col("player_id").isNotNull())
    .groupBy(
        "player_id",
        "player_name",
        "team_id",
        "team_name"
    )
    .agg(
        F.countDistinct("match_id").alias("matches_played"),
        F.count("*").alias("total_events"),

        F.sum(
            F.when(
                F.col("event_type_name") == "Pass",
                1
            ).otherwise(0)
        ).alias("passes"),

        # Tirs hors séances de tirs au but
        F.sum(
            F.when(
                (F.col("event_type_name") == "Shot") &
                (F.col("period") <= 4),
                1
            ).otherwise(0)
        ).alias("shots"),

        # Buts hors séances de tirs au but
        F.sum(
            F.when(
                (F.col("event_type_name") == "Shot") &
                (F.col("shot_outcome") == "Goal") &
                (F.col("period") <= 4),
                1
            ).otherwise(0)
        ).alias("goals"),

        # Expected Goals hors séances de tirs au but
        F.round(
            F.sum(
                F.when(
                    F.col("period") <= 4,
                    F.coalesce(
                        F.col("shot_xg"),
                        F.lit(0.0)
                    )
                ).otherwise(0.0)
            ),
            3
        ).alias("total_xg"),

        F.sum(
            F.when(
                F.col("under_pressure") == True,
                1
            ).otherwise(0)
        ).alias("events_under_pressure")
    )
    .withColumn(
        "xg_difference",
        F.round(
            F.col("goals") - F.col("total_xg"),
            3
        )
    )
    .withColumn(
        "_created_at",
        F.current_timestamp()
    )
)

(
    gold_player_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{CATALOG}.{SCHEMA}.gold_player_summary"
    )
)

player_count = gold_player_summary.count()

goal_count = gold_player_summary.agg(
    F.sum("goals").alias("total")
).first()["total"]

assert player_count > 0
assert goal_count > 0

print("gold_player_summary :", player_count, "joueurs")
print("Nombre total de buts attribués :", goal_count)

display(
    gold_player_summary.orderBy(
        F.desc("goals"),
        F.desc("total_xg"),
        F.desc("shots")
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

silver_matches = spark.table(
    "workspace.football_wc.silver_matches"
)

silver_events = spark.table(
    "workspace.football_wc.silver_events"
)

# Statistiques de chaque équipe dans chaque match
team_match_stats = (
    silver_events
    .filter(
        F.col("team_id").isNotNull() &
        (F.col("period") <= 4)
    )
    .groupBy("match_id", "team_id")
    .agg(
        F.sum(
            F.when(
                F.col("event_type_name") == "Pass",
                1
            ).otherwise(0)
        ).alias("passes"),

        F.sum(
            F.when(
                F.col("event_type_name") == "Shot",
                1
            ).otherwise(0)
        ).alias("shots"),

        F.round(
            F.sum(
                F.coalesce(F.col("shot_xg"), F.lit(0.0))
            ),
            3
        ).alias("xg")
    )
)

home_stats = team_match_stats.select(
    "match_id",
    F.col("team_id").alias("home_stats_team_id"),
    F.col("passes").alias("home_passes"),
    F.col("shots").alias("home_shots"),
    F.col("xg").alias("home_xg")
)

away_stats = team_match_stats.select(
    "match_id",
    F.col("team_id").alias("away_stats_team_id"),
    F.col("passes").alias("away_passes"),
    F.col("shots").alias("away_shots"),
    F.col("xg").alias("away_xg")
)

gold_match_summary = (
    silver_matches.alias("m")
    .join(
        home_stats.alias("h"),
        (F.col("m.match_id") == F.col("h.match_id")) &
        (F.col("m.home_team_id") == F.col("h.home_stats_team_id")),
        "left"
    )
    .join(
        away_stats.alias("a"),
        (F.col("m.match_id") == F.col("a.match_id")) &
        (F.col("m.away_team_id") == F.col("a.away_stats_team_id")),
        "left"
    )
    .select(
        "m.*",
        F.coalesce(F.col("h.home_passes"), F.lit(0)).alias("home_passes"),
        F.coalesce(F.col("h.home_shots"), F.lit(0)).alias("home_shots"),
        F.coalesce(F.col("h.home_xg"), F.lit(0.0)).alias("home_xg"),
        F.coalesce(F.col("a.away_passes"), F.lit(0)).alias("away_passes"),
        F.coalesce(F.col("a.away_shots"), F.lit(0)).alias("away_shots"),
        F.coalesce(F.col("a.away_xg"), F.lit(0.0)).alias("away_xg")
    )
    .withColumn("_gold_created_at", F.current_timestamp())
)

(
    gold_match_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.football_wc.gold_match_summary")
)

match_count = gold_match_summary.count()
assert match_count == 64

print("gold_match_summary :", match_count, "matchs")

display(
    gold_match_summary.orderBy("match_date", "kick_off")
)

# COMMAND ----------

EXPORT_PATH = "/Volumes/workspace/football_wc/landing/powerbi"

dbutils.fs.mkdirs(EXPORT_PATH)

gold_team_summary = spark.table(
    "workspace.football_wc.gold_team_summary"
)

gold_player_summary = spark.table(
    "workspace.football_wc.gold_player_summary"
)

gold_match_summary = spark.table(
    "workspace.football_wc.gold_match_summary"
)

gold_team_summary.toPandas().to_csv(
    f"{EXPORT_PATH}/gold_team_summary.csv",
    index=False,
    encoding="utf-8"
)

gold_player_summary.toPandas().to_csv(
    f"{EXPORT_PATH}/gold_player_summary.csv",
    index=False,
    encoding="utf-8"
)

gold_match_summary.toPandas().to_csv(
    f"{EXPORT_PATH}/gold_match_summary.csv",
    index=False,
    encoding="utf-8"
)

print("Exports Power BI créés :")
print(f"{EXPORT_PATH}/gold_team_summary.csv")
print(f"{EXPORT_PATH}/gold_player_summary.csv")
print(f"{EXPORT_PATH}/gold_match_summary.csv")