# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------
def flatten_app_details(df):
    return (df
        .withColumn("currency", F.col("price_overview.currency"))
        .withColumn("initial_price", F.col("price_overview.initial"))
        .withColumn("final_price", F.col("price_overview.final"))
        .withColumn("discount_percent", F.col("price_overview.discount_percent"))
        .withColumn("metacritic_score", F.col("metacritic.score"))
        .withColumn("coming_soon", F.col("release_date.coming_soon"))
        .withColumn("release_date", F.col("release_date.date"))
        .withColumn("genres", F.concat_ws(", ", F.col("genres.description")))
        .withColumn("categories", F.concat_ws(", ", F.col("categories.description")))
        .drop("price_overview", "metacritic")
    )

# COMMAND ----------
df_bronze_app_details = flatten_app_details(spark.read.table("steam_analytics.bronze.app_details"))
silver_table_name = "steam_analytics.silver.app_details"

# First execution
if not spark.catalog.tableExists(silver_table_name):
    w = Window.partitionBy("appid").orderBy(F.col("extracted_at_game").desc())
    w_asc = Window.partitionBy("appid").orderBy(F.col("extracted_at_game"))

    df_scd = (
        df_bronze_app_details
        .withColumn("row_num", F.row_number().over(w))
        .withColumn("is_current", F.col("row_num") == 1)
        .withColumn("start_date", F.col("extracted_at_game"))
        .withColumn("end_date",
            F.when(F.col("is_current") == True, F.lit(None).cast("timestamp"))
             .otherwise(F.lead("extracted_at_game").over(w_asc))
        )
        .drop("row_num")
    )

    df_scd.write.mode("overwrite").saveAsTable(silver_table_name)

# Incremental execution
else:

    w_dedup = Window.partitionBy("appid").orderBy(F.col("extracted_at_game").desc())
    df_bronze_app_details = (
        df_bronze_app_details
        .withColumn("rn", F.row_number().over(w_dedup))
        .filter("rn = 1")
        .drop("rn")
    )

    df_silver = spark.read.table(silver_table_name)
    
    df_bronze_staged = df_bronze_app_details.withColumn("merge_key", F.col("appid"))

    df_new = (df_bronze_staged
        .join(df_silver.filter("is_current = True"), on="appid", how="left_anti")
    )

    df_changed = (df_bronze_staged.alias("new")
        .join(df_silver.filter("is_current = True").alias("old"), on="appid")
        .where("new.extracted_at_game != old.extracted_at_game")
        .select("new.*")
        .withColumn("merge_key", F.lit(None).cast("integer")) 
    )
    
    df_update_triggers = df_changed.withColumn("merge_key", F.col("appid"))

    df_to_merge = df_new.unionByName(df_changed).unionByName(df_update_triggers)

    silver_table = DeltaTable.forName(spark, silver_table_name)

    (silver_table.alias("target").merge(
        df_to_merge.alias("source"),
        "target.appid = source.merge_key AND target.is_current = True"
    ).whenMatchedUpdate(set={
        "is_current": F.lit(False),
        "end_date": "source.extracted_at_game" 
    }).whenNotMatchedInsert(values={
        "appid": F.col("source.appid"),
        "extracted_at_game": F.col("source.extracted_at_game"),
        "is_free": F.col("source.is_free"),
        "short_description": F.col("source.short_description"),
        "header_image": F.col("source.header_image"),
        "developers": F.col("source.developers"),
        "publishers": F.col("source.publishers"),
        "currency": F.col("source.currency"),
        "initial_price": F.col("source.initial_price"),
        "final_price": F.col("source.final_price"),
        "discount_percent": F.col("source.discount_percent"),
        "metacritic_score": F.col("source.metacritic_score"),
        "genres": F.col("source.genres"),
        "categories": F.col("source.categories"),
        "coming_soon": F.col("source.coming_soon"),
        "release_date": F.col("source.release_date"),
        
        # Dimension fields
        "is_current": F.lit(True),
        "start_date": F.col("source.extracted_at_game"),
        "end_date": F.lit(None).cast("timestamp"),
    }).execute())