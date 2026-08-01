-- Databricks notebook source
-- Create the daily Gold table
CREATE OR REPLACE TABLE workspace.default.gold_weather_daily
USING DELTA
AS
SELECT
    weather_date,

    -- Daily average temperature
    ROUND(AVG(temperature_celsius), 2) AS avg_temperature_celsius,

    -- Lowest temperature recorded that day
    ROUND(MIN(temperature_celsius), 2) AS min_temperature_celsius,

    -- Highest temperature recorded that day
    ROUND(MAX(temperature_celsius), 2) AS max_temperature_celsius,

    -- Difference between daily maximum and minimum
    ROUND(
        MAX(temperature_celsius) - MIN(temperature_celsius),
        2
    ) AS temperature_range_celsius,

    -- Number of hourly records available
    COUNT(*) AS recorded_hours

FROM workspace.default.silver_weather_hourly
GROUP BY weather_date;

-- COMMAND ----------

-- Display the completed Gold table in chronological order
SELECT *
FROM workspace.default.gold_weather_daily
ORDER BY weather_date;

-- COMMAND ----------

-- Count the number of daily records stored in the Gold table
SELECT
    COUNT(*) AS gold_row_count
FROM workspace.default.gold_weather_daily;

-- COMMAND ----------

-- Check whether any weather date appears more than once
SELECT
    weather_date,
    COUNT(*) AS row_count
FROM workspace.default.gold_weather_daily
GROUP BY weather_date
HAVING COUNT(*) > 1;

-- COMMAND ----------

-- Inspect the Gold table format, location, size, and other metadata
DESCRIBE DETAIL workspace.default.gold_weather_daily;

-- COMMAND ----------

-- Validate the Gold table by returning only rows with incomplete hourly coverage or logically inconsistent temperature values
SELECT *
FROM workspace.default.gold_weather_daily
WHERE
    recorded_hours <> 24
    OR avg_temperature_celsius < min_temperature_celsius
    OR avg_temperature_celsius > max_temperature_celsius
    OR temperature_range_celsius < 0;

-- COMMAND ----------

-- Confirm that the stored temperature range equals
-- the daily maximum temperature minus the daily minimum temperature
SELECT *
FROM workspace.default.gold_weather_daily
WHERE ABS(
    temperature_range_celsius
    - (max_temperature_celsius - min_temperature_celsius)
) > 0.01;
