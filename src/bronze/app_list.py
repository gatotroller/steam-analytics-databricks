# Databricks notebook source
import sys
import os

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
from utils.steam_api_client import get_app_list

api_key = dbutils.secrets.get(scope="steam", key="api-key")

# COMMAND ----------
spark.sql("CREATE CATALOG IF NOT EXISTS steam_analytics")
spark.sql("CREATE SCHEMA IF NOT EXISTS steam_analytics.bronze")

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
