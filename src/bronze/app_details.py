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
spark.sql("CREATE VOLUME IF NOT EXISTS steam_analytics.bronze.landing")
spark.sql("CREATE VOLUME IF NOT EXISTS steam_analytics.bronze.checkpoint")
dbutils.fs.mkdirs("/Volumes/steam_analytics/bronze/landing/steam_app_details/")

# COMMAND ----------
from pyspark.sql import functions as F

# New Games
df_apps_list = spark.read.table("steam_analytics.bronze.app_list")
app_details_name = "steam_analytics.bronze.app_details"
app_changed_name = "steam_analytics.bronze.games_to_update"

if spark.catalog.tableExists(app_details_name):
    df_app_details = spark.read.table(app_details_name)
    df_new_games = (df_apps_list.alias("df_apps_list")
        .join(df_app_details.alias("df_app_details"), on="appid", how="left_anti")
        .select("df_apps_list.appid", "df_apps_list.extracted_at")
    )

else:
    df_new_games = df_apps_list.select("appid", "extracted_at")

df_new_games.write.mode("append").saveAsTable(app_changed_name)

# COMMAND ----------
df_games_to_update = spark.read.table(app_changed_name)

if spark.catalog.tableExists(app_details_name):
    df_app_details = spark.read.table(app_details_name)
    df_pending_games = (
        df_games_to_update
        .join(df_app_details, on=["appid", "extracted_at"], how="left_anti")
    )

else:
    df_pending_games = df_games_to_update

games = (
    df_pending_games
        .dropDuplicates(["appid"])
        .toPandas()
        .to_dict("records")
)
