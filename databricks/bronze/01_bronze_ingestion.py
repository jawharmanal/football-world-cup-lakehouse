# Databricks notebook source
import requests

url = (
    "https://raw.githubusercontent.com/"
    "statsbomb/open-data/master/data/matches/43/106.json"
)

response = requests.get(url, timeout=60)
response.raise_for_status()

matches = response.json()

print("HTTP status:", response.status_code)
print("Nombre de matchs:", len(matches))
print("Premier match_id:", matches[0]["match_id"])

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "football_wc"
VOLUME = "landing"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}"
)

LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

print("Schéma créé :", f"{CATALOG}.{SCHEMA}")
print("Volume créé :", LANDING_PATH)

# COMMAND ----------

import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = (
    "https://raw.githubusercontent.com/"
    "statsbomb/open-data/master/data"
)

paths = {
    "competitions": f"{LANDING_PATH}/statsbomb/competitions",
    "matches": f"{LANDING_PATH}/statsbomb/matches",
    "events": f"{LANDING_PATH}/statsbomb/events",
    "lineups": f"{LANDING_PATH}/statsbomb/lineups",
}

for path in paths.values():
    os.makedirs(path, exist_ok=True)


def download_file(relative_url, destination):
    response = requests.get(f"{BASE_URL}/{relative_url}", timeout=120)
    response.raise_for_status()

    with open(destination, "wb") as file:
        file.write(response.content)

    return destination


# Fichiers généraux
download_file(
    "competitions.json",
    f"{paths['competitions']}/competitions.json",
)

download_file(
    "matches/43/106.json",
    f"{paths['matches']}/matches_43_106.json",
)

# Un fichier events et lineups pour chaque match
jobs = []

for match in matches:
    match_id = match["match_id"]

    jobs.append((
        f"events/{match_id}.json",
        f"{paths['events']}/{match_id}.json",
    ))

    jobs.append((
        f"lineups/{match_id}.json",
        f"{paths['lineups']}/{match_id}.json",
    ))


with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [
        executor.submit(download_file, source, destination)
        for source, destination in jobs
    ]

    completed = 0

    for future in as_completed(futures):
        future.result()
        completed += 1

        if completed % 16 == 0:
            print(f"{completed}/{len(jobs)} fichiers téléchargés")


print("Téléchargement terminé")
print("Events :", len(os.listdir(paths["events"])))
print("Lineups :", len(os.listdir(paths["lineups"])))

# COMMAND ----------

from pyspark.sql import functions as F


def add_metadata(dataframe, source_name, match_id_from_file=False):
    result = (
        dataframe
        .withColumn("_source_system", F.lit("statsbomb"))
        .withColumn("_source_name", F.lit(source_name))
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    if match_id_from_file:
        result = result.withColumn(
            "match_id",
            F.regexp_extract(
                F.col("_source_file"),
                r"(\d+)\.json$",
                1,
            ).cast("long"),
        )

    return result


competitions_bronze = add_metadata(
    spark.read
        .option("multiLine", True)
        .json(paths["competitions"]),
    "competitions",
)

matches_bronze = add_metadata(
    spark.read
        .option("multiLine", True)
        .json(paths["matches"]),
    "matches",
)

events_bronze = add_metadata(
    spark.read
        .option("multiLine", True)
        .json(paths["events"]),
    "events",
    match_id_from_file=True,
)

lineups_bronze = add_metadata(
    spark.read
        .option("multiLine", True)
        .json(paths["lineups"]),
    "lineups",
    match_id_from_file=True,
)


tables = {
    "bronze_competitions": competitions_bronze,
    "bronze_matches": matches_bronze,
    "bronze_events": events_bronze,
    "bronze_lineups": lineups_bronze,
}


for table_name, dataframe in tables.items():
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    print(
        full_table_name,
        "->",
        spark.table(full_table_name).count(),
        "lignes",
    )