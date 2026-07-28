from pyspark.sql.functions import col, sum, min, max, to_date, hour

# Read the Bronze Delta table
bronze_weather_df = spark.table(
    "workspace.default.bronze_weather_hourly"
)

display(bronze_weather_df)

# Check for missing values
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

# Check for duplicate timestamps
duplicate_dates_df = (
    bronze_weather_df
    .groupBy("date")
    .count()
    .filter("count > 1")
)

display(duplicate_dates_df)

# Check the temperature range
temperature_range_df = bronze_weather_df.select(
    min("temperature_2m").alias("min_temperature"),
    max("temperature_2m").alias("max_temperature")
)

display(temperature_range_df)

# Clean and standardize the Bronze data
silver_weather_df = (
    bronze_weather_df
    .filter(col("date").isNotNull())
    .filter(col("temperature_2m").isNotNull())
    .dropDuplicates(["date"])
    .filter(
        (col("temperature_2m") >= -50) &
        (col("temperature_2m") <= 60)
    )
    .withColumnRenamed(
        "date",
        "weather_timestamp"
    )
    .withColumnRenamed(
        "temperature_2m",
        "temperature_celsius"
    )
    .withColumn(
        "weather_date",
        to_date("weather_timestamp")
    )
    .withColumn(
        "weather_hour",
        hour("weather_timestamp")
    )
)

# Validate the final Silver DataFrame
display(
    silver_weather_df.orderBy("weather_timestamp")
)

silver_weather_df.printSchema()

print("Bronze rows:", bronze_weather_df.count())
print("Silver rows:", silver_weather_df.count())

# Save the Silver Delta table
silver_weather_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_weather_hourly")

# Read and verify the saved Silver table
saved_silver_weather_df = spark.table(
    "workspace.default.silver_weather_hourly"
)

display(
    saved_silver_weather_df.orderBy("weather_timestamp")
)

saved_silver_weather_df.printSchema()

display(
    spark.sql("""
        describe detail workspace.default.silver_weather_hourly
    """)
)
