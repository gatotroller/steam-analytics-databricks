# Databricks notebook source
CREATE VIEW IF NOT EXISTS steam_analytics.gold.dim_apps AS (
  SELECT
    appid,
    name AS appName
  FROM app_list
);

CREATE VIEW IF NOT EXISTS steam_analytics.gold.fact_reviews AS (
  SELECT
    * EXCEPT (extracted_at)
  FROM steam_analytics.silver.reviews
);

CREATE VIEW IF NOT EXISTS steam_analytics.gold.fact_player_count AS (
  SELECT 
    appid,
    player_count,
    snapshot_date,
    snapshot_hour
  FROM steam_analytics.silver.player_count
);
