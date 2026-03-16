# Databricks notebook source
import sys
import os
import datetime
import pyspark.sql.functions as F

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
from utils.steam_api_client import get_app_list, get_logical_shift

api_key = dbutils.secrets.get(scope="steam", key="api-key")
spark.sql("CREATE CATALOG IF NOT EXISTS steam_analytics")
spark.sql("CREATE SCHEMA IF NOT EXISTS steam_analytics.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS steam_analytics.bronze.landing")
spark.sql("CREATE VOLUME IF NOT EXISTS steam_analytics.bronze.checkpoint")

app_list_name = "steam_analytics.bronze.app_list"
exists_table = spark.catalog.tableExists(app_list_name)

if exists_table:
    df_apps_old = spark.read.table(app_list_name)

    latest_batch = df_apps_old.agg(F.max("extracted_at")).collect()[0][0]
    latest_batch_dt = datetime.datetime.fromisoformat(latest_batch)
    last_shift = get_logical_shift(latest_batch_dt)

    current_time = datetime.datetime.now(datetime.timezone.utc)
    current_shift = get_logical_shift(current_time)

    # Check if you already execute the code between 4:00 pm - 3:59 pm
    # 4:00 pm is the time that steam refresh their catalog
    
    if current_shift == last_shift:
        dbutils.notebook.exit("Skipped: You already execute this code.")

    extraction_timestamp = current_time.isoformat()

# COMMAND ----------
apps = get_app_list(steam_key=api_key)

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, TimestampType

schema_app_list = StructType([
    StructField("appid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("last_modified", LongType(), True),
    StructField("price_change_number", LongType(), True),
    StructField("extracted_at", TimestampType(), True)
])

df_apps = spark.createDataFrame(apps, schema=schema_app_list)

if exists_table:
    df_changed = (
        df_apps.alias("new")
        .join(df_apps_old.alias("old"), on="appid", how="inner")
        .filter(F.col("new.last_modified") != F.col("old.last_modified"))
        .select("new.appid", "new.extracted_at")
    )

    df_changed.write.mode("append").saveAsTable("steam_analytics.bronze.games_to_update")

df_apps.write.mode("overwrite").saveAsTable("steam_analytics.bronze.app_list")
