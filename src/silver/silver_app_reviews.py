# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------
spark.sql("CREATE VOLUME IF NOT EXISTS steam_analytics.silver.checkpoint")
df_bronze_reviews = spark.readStream.table("steam_analytics.bronze.app_reviews")

spark.sql("CREATE SCHEMA IF NOT EXISTS steam_analytics.silver")

df_silver_reviews = (
    df_bronze_reviews
    .filter(F.col("total_reviews") > 0)
    .withColumn("review_score", F.round(F.col("total_positive") / F.col("total_reviews"), 4))
    .withColumn("extracted_at_Date", F.to_date("extracted_at"))
    .select(
        "appid",
        "review_score",
        "review_score_desc",
        "total_reviews",
        "extracted_at",
        "extracted_at_Date"
    )
)

(df_silver_reviews
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/steam_analytics/silver/checkpoint/steam_reviews/")
    .trigger(availableNow=True) 
    .toTable("steam_analytics.silver.reviews")
    .awaitTermination()
)

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