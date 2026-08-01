# Bangkok Weather Pipeline

An end-to-end data engineering project that ingests hourly Bangkok weather data from the Open-Meteo API and processes it through Bronze, Silver, and Gold layers using Python, PySpark, SQL, Delta Lake, and Databricks.

The final Gold table is used to power a Databricks AI/BI dashboard for daily temperature analysis.

## Project Goals

- Extract weather data from a public API
- Build a Bronze, Silver, and Gold data pipeline
- Store data in Delta tables
- Transform and validate data with PySpark
- Aggregate reporting metrics with SQL
- Build an interactive Databricks dashboard
- Prepare the pipeline for scheduled automation
- Apply practical data engineering and data-quality techniques

## Architecture

```text
Open-Meteo API
      ↓
Bronze Delta Table
      ↓
Silver Delta Table
      ↓
Gold Daily Table
      ↓
Databricks AI/BI Dashboard

## Dashboard

### Daily Average Temperature

![Daily Average Temperature](dashboard/daily_average_temperature.png)

### Daily Minimum, Average, and Maximum Temperature

![Daily Minimum, Average, and Maximum Temperature](dashboard/daily_min_avg_max_temperature.png)

### Daily Temperature Range

![Daily Temperature Range](dashboard/daily_temperature_range.png)
