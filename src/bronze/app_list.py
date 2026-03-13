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
spark.sql("CREATE CATALOG IF NOT EXISTS steam_analytics")
spark.sql("CREATE SCHEMA IF NOT EXISTS steam_analytics.bronze")
app_list_name = "steam_analytics.bronze.app_list"

# COMMAND ----------
apps = get_app_list(steam_key=api_key)

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

schema_app_list = StructType([
    StructField("appid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("last_modified", LongType(), True),
    StructField("price_change_number", LongType(), True)
])

if spark.catalog.tableExists(app_list_name):
    df_apps_list_old = spark.read.table(app_list_name)
else:
    df_apps_list_old = spark.createDataFrame([], schema=schema_app_list)
df_apps_list = spark.createDataFrame(apps, schema=schema_app_list)

# COMMAND ----------
from pyspark.sql import functions as F

# Games that its price change
df_changed_prices = (
    df_apps_list.alias("new")
        .join(df_apps_list_old.alias("old"), on="appid", how="inner")
        .where("new.price_change_number != old.price_change_number")
        .select("new.appid")
)

# Games_to_extract to use on app_details notebook
df_games_to_extract = df_changed_prices.withColumn("batch_date", F.current_timestamp())
df_games_to_extract.write.mode("append").saveAsTable("steam_analytics.bronze.games_to_update")

# COMMAND ----------
# Overwrite the new bronze table
df_apps_list.write.mode("overwrite").saveAsTable("steam_analytics.bronze.app_list")
