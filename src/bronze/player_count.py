# Databricks notebook source
import sys
sys.path.append("/Workspace/Users/eduardo.jafet31oct@gmail.com/.bundle/steam_analytics_platform/dev/files/src")

# COMMAND ----------
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession
from utils.steam_api_client import get_all_player_counts

spark = SparkSession.builder.getOrCreate()
api_key = dbutils.secrets.get(scope="steam", key="api-key")

# COMMAND ----------
df_apps = spark.read.table("steam_analytics.bronze.app_list")
games_list = df_apps.select("appid").toPandas().to_dict("records")
player_count = get_all_player_counts(steam_key=api_key, games=games_list)

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import datetime
import json

schema_player_count = StructType([
    StructField("appid", IntegerType(), True),
    StructField("player_count", StringType(), True),
    StructField("extracted_at", TimestampType(), True),
])

file_name = f"player_count_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
landing_zone = f"/Volumes/steam_analytics/bronze/landing/steam_player_count/{file_name}"

with open(landing_zone, "w", encoding="utf-8") as archivo_json:
    json.dump(player_count, archivo_json, ensure_ascii=False, indent=3)

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
