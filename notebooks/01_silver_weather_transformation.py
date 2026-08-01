# Databricks notebook source

from pyspark.sql.functions import (
    col,
    sum,
    min,
    max,
    to_date,
    hour
)


# Read the Bronze Delta table
bronze_weather_df = spark.table(
    "workspace.default.bronze_weather_hourly"
)


# Display the Bronze data in chronological order
display(
    bronze_weather_df.orderBy(
        "weather_timestamp"
    )
)


# Check for missing values in every Bronze column
display(
    bronze_weather_df.select(
        [
            sum(
                col(column).isNull().cast("int")
            ).alias(column)
            for column in bronze_weather_df.columns
        ]
    )
)


# Check for duplicate weather timestamps
duplicate_timestamps_df = (
    bronze_weather_df
    .groupBy("weather_timestamp")
    .count()
    .filter(col("count") > 1)
)

display(duplicate_timestamps_df)


# Check the overall temperature range
temperature_range_df = bronze_weather_df.select(
    min("temperature_celsius").alias(
        "min_temperature_celsius"
    ),
    max("temperature_celsius").alias(
        "max_temperature_celsius"
    )
)

display(temperature_range_df)


# Clean and enrich the Bronze data
silver_weather_df = (
    bronze_weather_df

    # Remove records with missing timestamps
    .filter(
        col("weather_timestamp").isNotNull()
    )

    # Remove records with missing temperatures
    .filter(
        col("temperature_celsius").isNotNull()
    )

    # Keep only one record for each hourly timestamp
    .dropDuplicates(
        ["weather_timestamp"]
    )

    # Remove temperatures outside a reasonable range
    .filter(
        (col("temperature_celsius") >= -50) &
        (col("temperature_celsius") <= 60)
    )

    # Extract the Bangkok calendar date
    .withColumn(
        "weather_date",
        to_date(
            col("weather_timestamp")
        )
    )

    # Extract the hour of the day
    .withColumn(
        "weather_hour",
        hour(
            col("weather_timestamp")
        )
    )
)


# Display the completed Silver DataFrame
display(
    silver_weather_df.orderBy(
        "weather_timestamp"
    )
)


# Inspect the Silver schema
silver_weather_df.printSchema()


# Compare the number of Bronze and Silver records
print(
    "Bronze rows:",
    bronze_weather_df.count()
)

print(
    "Silver rows:",
    silver_weather_df.count()
)


# Save the cleaned data as a Silver Delta table
(
    silver_weather_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.default.silver_weather_hourly"
    )
)


# Read the saved Silver Delta table
saved_silver_weather_df = spark.table(
    "workspace.default.silver_weather_hourly"
)


# Verify the saved Silver records
display(
    saved_silver_weather_df.orderBy(
        "weather_timestamp"
    )
)


# Inspect the saved Silver schema
saved_silver_weather_df.printSchema()


# Verify that each date contains the expected hourly records
display(
    saved_silver_weather_df
    .groupBy("weather_date")
    .count()
    .orderBy("weather_date")
)


# Inspect the Silver Delta table metadata
display(
    spark.sql("""
        DESCRIBE DETAIL
        workspace.default.silver_weather_hourly
    """)
)
