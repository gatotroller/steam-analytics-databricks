# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------
df_bronze_app_details = spark.read.table("steam_analytics.bronze.app_details")

df_silver_app_details = (
    df_bronze_app_details
    .select(
        "appid",
        F.col("extracted_at_game").alias("extracted_at"),
        "is_free",
        "short_description",
        "header_image",

        # Arrays
        F.concat_ws(", ", F.col("developers")).alias("developers"),
        F.concat_ws(", ", F.col("publishers")).alias("publishers"),

        # Price
        F.col("price_overview.currency").alias("currency"),
        (F.col("price_overview.initial") / 100).alias("initial_price"),
        (F.col("price_overview.final") / 100).alias("final_price"),
        F.col("price_overview.discount_percent").alias("discount_percent"),

        # Score
        F.col("metacritic.score").alias("metacritic_score"),

        # Arrays.Dict
        F.concat_ws(", ", F.col("categories.description")).alias("categories"),
        F.concat_ws(", ", F.col("genres.description")).alias("genres"),

        # Release Date
        F.col("release_date.coming_soon").alias("coming_soon"),
        F.col("release_date.date").alias("release_date")
    )
)
