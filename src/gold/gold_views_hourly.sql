-- Databricks notebook source
CREATE VIEW IF NOT EXISTS steam_analytics.gold.fact_player_count AS (
  SELECT 
    appid,
    player_count,
    snapshot_date,
    snapshot_hour
  FROM steam_analytics.silver.player_count
);