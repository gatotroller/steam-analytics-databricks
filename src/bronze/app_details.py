# Databricks notebook source
import sys
import os
import asyncio

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
from utils.steam_api_client import get_all_apps_details

# COMMAND ----------
app_list_name = "steam_analytics.bronze.app_list"
app_details_name = "steam_analytics.silver.app_details"

app_list = spark.read.table(app_list_name)

if spark.catalog.tableExists(app_details_name):
    app_details = spark.read.table(app_details_name)
    df_apps = app_list.join(app_details, on="appid", how="left_anti")
else:
    df_apps = app_list

games_list = df_apps.select("appid").toPandas().to_dict("records")