# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.window as Window
from delta.tables import DeltaTable

# COMMAND ----------
df_bronze_app_details = spark.read.table("steam_analytics.bronze.app_details")
window_app_details = Window.partitionBy("appid").orderBy(F.col("extracted_at_game").desc())

# First execution
if not spark.catalog.tableExists("steam_analytics.silver.app_details"):
    df_scd_app_details = (
        df_bronze_app_details
        .withColumn("row_num", F.row_number().over(window_app_details))
        .withColumn("is_current", F.col("row_num") == 1)
        .withColumn("start_date", F.col("extracted_at_game"))
        .withColumn(
            "end_date",
            F.when(
                F.col("is_current") == True, None
            )
            .otherwise(
                F.lead("extracted_at_game").over(Window.partitionBy("appid").orderBy("extracted_at_game"))
            )
        )
        .drop("row_num")
    )

    df_scd_app_details.write.mode("overwrite").saveAsTable("steam_analytics.silver.app_details")

else:
    df_silver_app_details = spark.read.table("steam_analytics.silver.app_details")

    df_new_app_ids = (df_bronze_app_details
        .join(df_silver_app_details.filter("is_current = True"), on="appid", how="left_anti")
    )

    # Or records where data actually changed
    df_changed = (df_bronze_app_details.alias("new")
        .join(df_silver_app_details.filter("is_current = True").alias("old"), on="appid")
        .where(F.col("new.extracted_at_game") != F.col("old.extracted_at_game"))
        .select("new.*")
    )

    df_to_merge = df_new_app_ids.unionByName(df_changed)

    silver_table = DeltaTable.forName(spark, "steam_analytics.silver.app_details")

    silver_table.alias("target").merge(
        df_to_merge
            .withColumn("is_current", F.lit(True))
            .withColumn("start_date", F.col("extracted_at_game"))
            .withColumn("end_date", F.lit(None).cast("timestamp"))
            .alias("source"),
        "target.appid = source.appid AND target.is_current = True"
    ).whenMatchedUpdate(set={
        "is_current": F.lit(False),
        "end_date": F.current_timestamp()
    }).whenNotMatchedInsert(values={
        "appid": "source.appid",
        "is_current": F.lit(True),
        "start_date": "source.extracted_at_game",
        "end_date": F.lit(None),
        # ... all other columns
    }).execute()


df_silver_app_details = (
    df_bronze_app_details
    .select(
        "appid",
        "extracted_at_game",
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
        F.col("release_date.date").alias("release_date"),
    )
)

# COMMAND ----------
