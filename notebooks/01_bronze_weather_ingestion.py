# Path to the raw CSV file uploaded into the Databricks Volume
file_path = "/Volumes/workspace/default/bangkok_weather/bangkok_weather_raw.csv"


# Read the CSV into a temporary PySpark DataFrame
# header=True uses the first row as column names
# inferSchema=True lets Spark detect types such as timestamp and double
raw_weather_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(file_path)
)


# Preview the raw PySpark DataFrame created from the CSV
display(raw_weather_df)


# Save the raw PySpark DataFrame as a persistent Bronze Delta table
# overwrite replaces the existing table when the notebook is rerun
raw_weather_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_weather_hourly")


# Read the saved Bronze Delta table back into a PySpark DataFrame
# This confirms that the table was created successfully
bronze_weather_df = spark.table(
    "workspace.default.bronze_weather_hourly"
)


# Preview the data loaded from the Bronze Delta table
display(bronze_weather_df)


# Display metadata about the Bronze table
# The format column should show "delta"
display(
    spark.sql("""
        describe detail workspace.default.bronze_weather_hourly
    """)
)

# Print the Bronze DataFrame schema to verify column names, data types, and nullability
bronze_weather_df.printSchema()
