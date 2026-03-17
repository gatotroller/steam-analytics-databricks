# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------
spark.sql("CREATE VOLUME IF NOT EXISTS steam_analytics.silver.checkpoint")
df_bronze_player_count = spark.readStream.table("steam_analytics.bronze.player_count")

df_silver_player_count = (
    df_bronze_player_count
    .filter(F.col("player_count") > 0)
    .withColumn("extracted_at_Date", F.to_date("extracted_at"))
)

(df_silver_player_count
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/steam_analytics/silver/checkpoint/steam_player_count/")
    .trigger(availableNow=True) 
    .toTable("steam_analytics.silver.player_count")
    .awaitTermination()
)
