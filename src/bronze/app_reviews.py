# Databricks notebook source
import sys
import os
import pyspark.sql.functions as F
import datetime
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
from utils.steam_api_client import get_all_reviews, get_logical_shift
dbutils.fs.mkdirs("/Volumes/steam_analytics/bronze/landing/steam_app_reviews/")

# COMMAND ----------
df_apps = spark.read.table("steam_analytics.bronze.app_list")
games_list = df_apps.select("appid").toPandas().to_dict("records")

app_reviews_name = "steam_analytics.bronze.app_reviews"
exists_table = spark.catalog.tableExists(app_reviews_name)

if exists_table:
    df_reviews = spark.read.table(app_reviews_name)

    latest_batch = df_reviews.agg(F.max("extracted_at")).collect()[0][0]
    latest_batch_dt = datetime.datetime.fromisoformat(latest_batch)
    last_shift = get_logical_shift(latest_batch_dt)

    current_time = datetime.datetime.now(datetime.timezone.utc)
    current_shift = get_logical_shift(current_time)

    # Check if you already execute the code between 4:00 pm - 3:59 pm
    # 4:00 pm is the time that steam refresh their catalog
    
    if current_shift == last_shift:
        dbutils.notebook.exit("Skipped: You already execute this code.")

# COMMAND ----------
# IMPORTANT: await seems like an error just in local not in Databricks UI
reviews = await get_all_reviews(games=games_list) # pyright: ignore

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, TimestampType
import json

schema_reviews = StructType([
    StructField("appid", IntegerType(), True),
    StructField("review_score", IntegerType(), True),
    StructField("review_score_desc", StringType(), True),
    
    StructField("total_positive", LongType(), True),
    StructField("total_negative", LongType(), True),
    StructField("total_reviews", LongType(), True),
    
    StructField("extracted_at", TimestampType(), True) 
])

file_name = f"app_review_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
landing_zone = f"/Volumes/steam_analytics/bronze/landing/steam_app_reviews/{file_name}"

with open(landing_zone, "w", encoding="utf-8") as json_file:
    json.dump(reviews, json_file, ensure_ascii=False)

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.cleanSource", "delete")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .schema(schema_reviews)
    .load("/Volumes/steam_analytics/bronze/landing/steam_app_reviews/")
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/steam_analytics/bronze/checkpoint/steam_app_reviews/")
    .trigger(availableNow=True)
    .toTable(app_reviews_name)
    .awaitTermination()
)
