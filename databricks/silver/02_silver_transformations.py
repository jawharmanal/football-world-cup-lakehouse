# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "football_wc"

bronze_matches = spark.table(
    f"{CATALOG}.{SCHEMA}.bronze_matches"
)

bronze_events = spark.table(
    f"{CATALOG}.{SCHEMA}.bronze_events"
)

bronze_lineups = spark.table(
    f"{CATALOG}.{SCHEMA}.bronze_lineups"
)

print("Bronze matches :", bronze_matches.count())
print("Bronze events  :", bronze_events.count())
print("Bronze lineups :", bronze_lineups.count())

# COMMAND ----------

silver_matches = (
    bronze_matches
    .select(
        F.col("match_id").cast("long").alias("match_id"),
        F.to_date("match_date").alias("match_date"),
        F.col("kick_off"),
        F.col("match_week").cast("integer").alias("match_week"),

        F.col("competition.competition_id")
            .cast("long")
            .alias("competition_id"),
        F.col("competition.competition_name")
            .alias("competition_name"),
        F.col("season.season_id")
            .cast("long")
            .alias("season_id"),
        F.col("season.season_name")
            .alias("season_name"),

        F.col("home_team.home_team_id")
            .cast("long")
            .alias("home_team_id"),
        F.col("home_team.home_team_name")
            .alias("home_team_name"),
        F.col("away_team.away_team_id")
            .cast("long")
            .alias("away_team_id"),
        F.col("away_team.away_team_name")
            .alias("away_team_name"),

        F.col("home_score").cast("integer").alias("home_score"),
        F.col("away_score").cast("integer").alias("away_score"),

        F.col("competition_stage.id")
            .cast("long")
            .alias("stage_id"),
        F.col("competition_stage.name")
            .alias("stage_name"),

        F.col("stadium.id").cast("long").alias("stadium_id"),
        F.col("stadium.name").alias("stadium_name"),
        F.col("referee.id").cast("long").alias("referee_id"),
        F.col("referee.name").alias("referee_name"),

        F.col("_source_file"),
        F.current_timestamp().alias("_processed_at"),
    )
    .filter(F.col("match_id").isNotNull())
    .dropDuplicates(["match_id"])
    .withColumn(
        "winner",
        F.when(
            F.col("home_score") > F.col("away_score"),
            F.col("home_team_name"),
        )
        .when(
            F.col("away_score") > F.col("home_score"),
            F.col("away_team_name"),
        )
        .otherwise(F.lit("Draw")),
    )
    .withColumn(
        "total_goals",
        F.col("home_score") + F.col("away_score"),
    )
)

(
    silver_matches.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_matches")
)

silver_matches_count = silver_matches.count()

assert silver_matches_count == 64
assert silver_matches.select("match_id").distinct().count() == 64

print("silver_matches :", silver_matches_count, "lignes")
display(
    silver_matches.select(
        "match_date",
        "home_team_name",
        "away_team_name",
        "home_score",
        "away_score",
        "winner",
        "stage_name",
    ).orderBy("match_date")
)

# COMMAND ----------

silver_events = (
    bronze_events
    .select(
        F.col("match_id").cast("long").alias("match_id"),
        F.col("id").alias("event_id"),
        F.col("index").cast("integer").alias("event_index"),
        F.col("period").cast("integer").alias("period"),
        F.col("timestamp"),
        F.col("minute").cast("integer").alias("minute"),
        F.col("second").cast("integer").alias("second"),

        F.col("type.id").cast("long").alias("event_type_id"),
        F.col("type.name").alias("event_type_name"),

        F.col("team.id").cast("long").alias("team_id"),
        F.col("team.name").alias("team_name"),
        F.col("player.id").cast("long").alias("player_id"),
        F.col("player.name").alias("player_name"),
        F.col("position.id").cast("long").alias("position_id"),
        F.col("position.name").alias("position_name"),

        F.col("possession").cast("integer").alias("possession"),
        F.col("possession_team.id")
            .cast("long")
            .alias("possession_team_id"),
        F.col("possession_team.name")
            .alias("possession_team_name"),

        F.col("play_pattern.id")
            .cast("long")
            .alias("play_pattern_id"),
        F.col("play_pattern.name")
            .alias("play_pattern_name"),

        F.col("location")[0].cast("double").alias("location_x"),
        F.col("location")[1].cast("double").alias("location_y"),

        F.col("duration").cast("double").alias("duration"),
        F.coalesce(
            F.col("under_pressure"),
            F.lit(False),
        ).alias("under_pressure"),
        F.coalesce(
            F.col("counterpress"),
            F.lit(False),
        ).alias("counterpress"),

        # Informations sur les passes
        F.col("pass.recipient.id")
            .cast("long")
            .alias("pass_recipient_id"),
        F.col("pass.recipient.name")
            .alias("pass_recipient_name"),
        F.col("pass.length").cast("double").alias("pass_length"),
        F.col("pass.angle").cast("double").alias("pass_angle"),
        F.col("pass.height.name").alias("pass_height"),
        F.col("pass.outcome.name").alias("pass_outcome"),
        F.col("pass.end_location")[0]
            .cast("double")
            .alias("pass_end_x"),
        F.col("pass.end_location")[1]
            .cast("double")
            .alias("pass_end_y"),

        # Informations sur les tirs
        F.col("shot.statsbomb_xg")
            .cast("double")
            .alias("shot_xg"),
        F.col("shot.outcome.name").alias("shot_outcome"),
        F.col("shot.type.name").alias("shot_type"),
        F.col("shot.body_part.name").alias("shot_body_part"),
        F.col("shot.technique.name").alias("shot_technique"),
        F.col("shot.end_location")[0]
            .cast("double")
            .alias("shot_end_x"),
        F.col("shot.end_location")[1]
            .cast("double")
            .alias("shot_end_y"),

        F.col("_source_file"),
        F.current_timestamp().alias("_processed_at"),
    )
    .filter(F.col("event_id").isNotNull())
    .filter(F.col("match_id").isNotNull())
    .dropDuplicates(["event_id"])
)

(
    silver_events.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_events")
)

event_count = silver_events.count()
distinct_event_count = (
    silver_events.select("event_id").distinct().count()
)

assert event_count == distinct_event_count
assert silver_events.filter(
    F.col("match_id").isNull()
).count() == 0

print("silver_events :", event_count, "lignes")
print("Événements uniques :", distinct_event_count)

display(
    silver_events.groupBy("event_type_name")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

from pyspark.sql import functions as F

silver_lineups = (
    bronze_lineups
    .select(
        F.col("match_id").cast("long").alias("match_id"),
        F.col("team_id").cast("long").alias("team_id"),
        F.col("team_name"),
        F.explode("lineup").alias("player"),
        F.col("_source_file")
    )
    .select(
        "match_id",
        "team_id",
        "team_name",
        F.col("player.player_id").cast("long").alias("player_id"),
        F.col("player.player_name").alias("player_name"),
        F.col("player.player_nickname").alias("player_nickname"),
        F.col("player.jersey_number").cast("integer").alias("jersey_number"),
        F.col("player.country.id").cast("long").alias("country_id"),
        F.col("player.country.name").alias("country_name"),
        F.col("player.cards").alias("cards"),
        F.col("player.positions").alias("positions"),
        "_source_file",
        F.current_timestamp().alias("_processed_at")
    )
    .filter(F.col("match_id").isNotNull())
    .filter(F.col("player_id").isNotNull())
    .dropDuplicates(["match_id", "team_id", "player_id"])
)

(
    silver_lineups.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_lineups")
)

lineup_count = silver_lineups.count()
match_count = silver_lineups.select("match_id").distinct().count()
player_count = silver_lineups.select("player_id").distinct().count()

assert match_count == 64
assert lineup_count > 0

print("silver_lineups :", lineup_count, "lignes")
print("Matchs couverts :", match_count)
print("Joueurs uniques :", player_count)

display(
    silver_lineups.select(
        "match_id",
        "team_name",
        "player_name",
        "jersey_number",
        "country_name"
    ).orderBy("match_id", "team_name", "jersey_number")
)

# COMMAND ----------

home_teams = bronze_matches.select(
    F.col("home_team.home_team_id").cast("long").alias("team_id"),
    F.col("home_team.home_team_name").alias("team_name")
)

away_teams = bronze_matches.select(
    F.col("away_team.away_team_id").cast("long").alias("team_id"),
    F.col("away_team.away_team_name").alias("team_name")
)

silver_teams = (
    home_teams
    .unionByName(away_teams)
    .filter(F.col("team_id").isNotNull())
    .dropDuplicates(["team_id"])
    .withColumn("_processed_at", F.current_timestamp())
)

(
    silver_teams.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_teams")
)

team_count = silver_teams.count()

assert team_count == 32

print("silver_teams :", team_count, "équipes")

display(
    silver_teams.orderBy("team_name")
)