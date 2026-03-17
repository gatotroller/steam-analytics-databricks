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
df_games_to_update = spark.read.table(app_changed_name)

if spark.catalog.tableExists(app_details_name):
    df_app_details = spark.read.table(app_details_name)
    
    df_new_games = (df_apps_list
        .join(df_app_details, on="appid", how="left_anti")
        .select("appid", "extracted_at")
    )
else:
    df_new_games = df_apps_list.select("appid", "extracted_at")
    
if spark.catalog.tableExists(app_changed_name):
    df_games_to_update = spark.read.table(app_changed_name)

    if spark.catalog.tableExists(app_details_name):
        df_pending_updates = (df_games_to_update
            .join(df_app_details, on=["appid", "extracted_at"], how="left_anti")
        )
    else:
        df_pending_updates = df_games_to_update
    
    df_pending_games = df_pending_updates.unionByName(df_new_games, allowMissingColumns=True)
    
else:
    df_pending_games = df_new_games

games = (
    df_pending_games
        .dropDuplicates(["appid"])
        .toPandas()
        .to_dict("records")
)

# COMMAND ----------
try:
    app_details = await get_all_apps_details(games=games)

finally:
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, 
        BooleanType, ArrayType, TimestampType, LongType
    )

    schema_app_details = StructType([
        StructField("appid", IntegerType(), True),
        StructField("extracted_at", TimestampType(), True),
        StructField("is_free", BooleanType(), True),
        StructField("short_description", StringType(), True),
        StructField("header_image", StringType(), True),
        
        StructField("developers", ArrayType(StringType()), True),
        StructField("publishers", ArrayType(StringType()), True),
        
        # Price overview
        StructField("price_overview", StructType([
            StructField("currency", StringType(), True),
            StructField("initial", LongType(), True),
            StructField("final", LongType(), True),
            StructField("discount_percent", IntegerType(), True),
        ]), True),
        
        StructField("metacritic", StructType([
            StructField("score", IntegerType(), True)
        ]), True),
        
        StructField("genres", ArrayType(
            StructType([
                StructField("description", StringType(), True)
            ])
        ), True),
        
        StructField("categories", ArrayType(
            StructType([
                StructField("description", StringType(), True)
            ])
        ), True),
        
        StructField("release_date", StructType([
            StructField("coming_soon", BooleanType(), True),
            StructField("date", StringType(), True)
        ]), True),
        
        StructField("extracted_at_game", TimestampType(), True)
    ])

    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.cleanSource", "delete")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .schema(schema_app_details)
        .load("/Volumes/steam_analytics/bronze/landing/steam_app_details/")
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/steam_analytics/bronze/checkpoint/steam_app_details/") 
        .trigger(availableNow=True)
        .toTable("steam_analytics.bronze.app_details")
        .awaitTermination()
    )
