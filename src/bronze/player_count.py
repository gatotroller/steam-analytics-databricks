# Databricks notebook source
import sys
import os
import asyncio

#LOCAL
try:
    path_current = os.path.dirname(os.path.abspath(__file__))
#DATABRICKS UI
except NameError:
    path_current = os.getcwd()

path_src = os.path.abspath(os.path.join(path_current, ".."))

if path_src not in sys.path:
    sys.path.append(path_src)

# COMMAND ----------
from utils.steam_api_client import get_all_player_counts

api_key = dbutils.secrets.get(scope="steam", key="api-key")
dbutils.fs.mkdirs("/Volumes/steam_analytics/bronze/landing/steam_player_count/")
spark.sql("CREATE VOLUME IF NOT EXISTS steam_analytics.bronze.landing")
spark.sql("CREATE VOLUME IF NOT EXISTS steam_analytics.bronze.checkpoint")

# COMMAND ----------
df_apps = spark.read.table("steam_analytics.bronze.app_list")
games_list = df_apps.select("appid").toPandas().to_dict("records")

# IMPORTANT: await seems like an error just in local not in Databricks UI
player_count = await get_all_player_counts(api_key=api_key, games=games_list)

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
import datetime
import json

schema_player_count = StructType([
    StructField("appid", IntegerType(), True),
    StructField("player_count", IntegerType(), True),
    StructField("extracted_at", TimestampType(), True),
])

file_name = f"player_count_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
landing_zone = f"/Volumes/steam_analytics/bronze/landing/steam_player_count/{file_name}"

with open(landing_zone, "w", encoding="utf-8") as json_file:
    json.dump(player_count, json_file, ensure_ascii=False, indent=3)

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .schema(schema_player_count)
    .load("/Volumes/steam_analytics/bronze/landing/steam_player_count/")
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/steam_analytics/bronze/checkpoint/steam_player_count/")
    .trigger(availableNow=True)
    .toTable("steam_analytics.bronze.player_count")
)
