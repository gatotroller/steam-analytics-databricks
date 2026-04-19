-- Databricks notebook source
CREATE VIEW IF NOT EXISTS steam_analytics.gold.dim_apps AS (
  SELECT
    appid,
    name AS appName
  FROM steam_analytics.silver.app_list
);

CREATE VIEW IF NOT EXISTS steam_analytics.gold.fact_reviews AS (
  SELECT
    * EXCEPT (extracted_at)
  FROM steam_analytics.silver.reviews
);
