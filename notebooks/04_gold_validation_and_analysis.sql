-- Databricks notebook source
-- View the final Gold reporting table
SELECT
    weather_date,
    avg_temperature_celsius,
    min_temperature_celsius,
    max_temperature_celsius,
    temperature_range_celsius,
    recorded_hours
FROM workspace.default.gold_weather_daily
ORDER BY weather_date;

-- COMMAND ----------

-- Prepare daily average temperature data for a line chart
SELECT
    weather_date,
    avg_temperature_celsius
FROM workspace.default.gold_weather_daily
ORDER BY weather_date;

-- COMMAND ----------

-- Compare daily minimum, average, and maximum temperatures
SELECT
    weather_date,
    min_temperature_celsius,
    avg_temperature_celsius,
    max_temperature_celsius
FROM workspace.default.gold_weather_daily
ORDER BY weather_date;

-- COMMAND ----------

-- Show the daily temperature range
SELECT
    weather_date,
    temperature_range_celsius
FROM workspace.default.gold_weather_daily
ORDER BY weather_date;


-- COMMAND ----------

-- Summarize the main temperature statistics across the dataset
SELECT
    ROUND(
        AVG(avg_temperature_celsius),
        2
    ) AS overall_avg_temperature_celsius,

    MAX(max_temperature_celsius)
        AS highest_recorded_temperature_celsius,

    MIN(min_temperature_celsius)
        AS lowest_recorded_temperature_celsius,

    ROUND(
        AVG(temperature_range_celsius),
        2
    ) AS avg_daily_temperature_range_celsius

FROM workspace.default.gold_weather_daily;

-- COMMAND ----------

-- Identify the hottest day by daily maximum temperature
SELECT
    weather_date,
    max_temperature_celsius
FROM workspace.default.gold_weather_daily
ORDER BY max_temperature_celsius DESC
LIMIT 1;

-- COMMAND ----------

-- Identify the coolest day by daily minimum temperature
SELECT
    weather_date,
    min_temperature_celsius
FROM workspace.default.gold_weather_daily
ORDER BY min_temperature_celsius ASC
LIMIT 1;

-- COMMAND ----------

-- Identify the day with the greatest temperature variation
SELECT
    weather_date,
    temperature_range_celsius
FROM workspace.default.gold_weather_daily
ORDER BY temperature_range_celsius DESC
LIMIT 1;