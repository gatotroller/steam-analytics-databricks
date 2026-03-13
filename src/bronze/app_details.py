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
from utils.steam_api_client import get_all_apps_details
dbutils.fs.mkdirs("/Volumes/steam_analytics/bronze/landing/steam_app_details/")
spark.sql("CREATE VOLUME IF NOT EXISTS steam_analytics.bronze.landing")
spark.sql("CREATE VOLUME IF NOT EXISTS steam_analytics.bronze.checkpoint")

# COMMAND ----------
from pyspark.sql import functions as F

# New Games
df_apps_list = spark.read.table("steam_analytics.bronze.app_list")
app_details_name = "steam_analytics.bronze.app_details"

if spark.catalog.tableExists(app_details_name):
    df_app_details = spark.read.table(app_details_name)
    df_new_games = df_apps_list.join(df_app_details, on="appid", how="left_anti")

else:
    df_new_games = df_apps_list

df_games_to_extract = df_new_games.select(F.col("appid")).withColumn("batch_date", F.current_date())
df_games_to_extract.write.mode("append").saveAsTable("steam_analytics.bronze.games_to_update")

# COMMAND ----------
df_apps = spark.read.table("steam_analytics.bronze.games_to_update")
latest_batch = df_apps.agg(F.max("batch_date")).collect()[0][0]
df_current_queue = df_apps.filter(F.col("batch_date") == latest_batch)

app_details_name = "steam_analytics.bronze.app_details"

if spark.catalog.tableExists(app_details_name):
    df_app_details = spark.read.table(app_details_name)
    df_already_processed = df_app_details.filter(F.col("extracted_at") >= F.lit(latest_batch))
    df_pending_games = df_current_queue.join(df_already_processed, on="appid", how="left_anti")

else:
    df_pending_games = df_current_queue

games = (
    df_apps
        .dropDuplicates(["appid", "batch_date"])
        .toPandas()
        .to_dict("records")
)

