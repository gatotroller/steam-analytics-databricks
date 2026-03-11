# Databricks notebook source
import sys
sys.path.append("/Workspace/Users/eduardo.jafet31oct@gmail.com/.bundle/steam_analytics_platform/dev/files/src")

# COMMAND ----------
from databricks.sdk.runtime import dbutils
from utils.steam_api_client import get_app_list
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
api_key = dbutils.secrets.get(scope="steam", key="api-key")

# COMMAND ----------
spark.sql("CREATE CATALOG IF NOT EXISTS steam_analytics")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

apps = get_app_list(steam_key=api_key)

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

schema_app_list = StructType([
    StructField("appid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("last_modified", LongType(), True),
    StructField("price_change_number", LongType(), True)
])

df_apps_list = spark.createDataFrame(apps, schema=schema_app_list)
df_apps_list.write.mode("overwrite").saveAsTable("steam_analytics.bronze.app_list")
