# Databricks notebook source
CREATE OR REFRESH MATERIALIZED VIEW steam_analytics.gold.gold_app_details
AS
SELECT
  appid,
  cast(extracted_at_game AS date),
  is_free,
  short_description,
  header_image,
  release_date,
  currency,
  initial_price / 100 AS initial_price,
  final_price / 100 AS final_price,
  discount_percent,
  metacritic_score,
  coming_soon,
  is_current
FROM steam_analytics.silver.app_details;

CREATE OR REFRESH MATERIALIZED VIEW steam_analytics.gold.Bridge_App_Developer
AS
SELECT DISTINCT
  appid,
  explode(developers) AS developers
FROM steam_analytics.silver.app_details;

CREATE OR REFRESH MATERIALIZED VIEW steam_analytics.gold.Bridge_App_Publisher
AS
SELECT DISTINCT
  appid,
  explode(publishers) AS publishers
FROM steam_analytics.silver.app_details;

CREATE OR REFRESH MATERIALIZED VIEW steam_analytics.gold.Bridge_App_Genre AS
SELECT DISTINCT
    appid,
    TRIM(genre_name) AS genre_name
FROM steam_analytics.silver.app_details
LATERAL VIEW explode(split(genres, ',')) AS genre_name
WHERE genre_name != '';

CREATE OR REFRESH MATERIALIZED VIEW steam_analytics.gold.Bridge_App_Category AS
SELECT DISTINCT
    appid,
    TRIM(category_name) AS category_name
FROM steam_analytics.silver.app_details
LATERAL VIEW explode(split(categories, ',')) AS category_name
WHERE category_name != '';

CREATE OR REFRESH MATERIALIZED VIEW steam_analytics.gold.dim_date AS
WITH date_series AS (
  SELECT explode(sequence(
    to_date('2020-01-01'), 
    to_date('2030-12-31'), 
    interval 1 day
  )) as date_val
)
SELECT
  cast(date_format(date_val, 'yyyyMMdd') as INT) as date_key,
  date_val as full_date,

  year(date_val) as year,
  month(date_val) as month_num,
  weekofyear(date_val) as week_num,
  day(date_val) as day_of_month,
  
  date_format(date_val, 'yyyy-MM') as year_month,
  concat(year(date_val), '-', lpad(weekofyear(date_val), 2, '0')) as year_week,
  
  date_format(date_val, 'EEEE') as day_name,
  CASE WHEN dayofweek(date_val) IN (1, 7) THEN true ELSE false END as is_weekend
FROM date_series;