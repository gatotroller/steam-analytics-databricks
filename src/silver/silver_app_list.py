# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------
df_bronze_apps = spark.read.table("steam_analytics.bronze.app_list")

df_silver_apps = (
    df_bronze_apps
    .filter(
        (F.col("name").isNotNull()) &
        (F.col("name") != "")
    )
    .withColumn("last_modified", F.to_timestamp(F.from_unixtime("last_modified")))
    .select(
        "appid",
        "name",
        "last_modified"
    )
)

df_silver_apps.write.mode("overwrite").saveAsTable("steam_analytics.silver.app_list")
