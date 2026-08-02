# Databricks notebook source

# Import date tools for dynamic API dates
from datetime import date, timedelta

# Import the time module so the notebook can pause between retry attempts
import time

# Import requests for sending HTTP requests to the Open-Meteo API
import requests


# Open-Meteo forecast API endpoint
url = "https://api.open-meteo.com/v1/forecast"


# Calculate the date range at runtime
start_date = date.today()
end_date = start_date + timedelta(days=6)


# Define the request parameters
params = {
    # Bangkok latitude
    "latitude": 13.7563,

    # Bangkok longitude
    "longitude": 100.5018,

    # Request hourly air temperature
    "hourly": "temperature_2m",

    # Return timestamps in Bangkok local time
    "timezone": "Asia/Bangkok",

    # First date included in the request
    "start_date": start_date.isoformat(),

    # Last date included in the request
    "end_date": end_date.isoformat()
}


# Maximum number of times the notebook will try the API request
max_attempts = 3


# Try the API request up to three times
for attempt in range(1, max_attempts + 1):

    try:
        # Send the HTTP GET request
        #
        # timeout=60 means Databricks will wait up to 60 seconds
        # for the API response before treating the request as failed
        response = requests.get(
            url,
            params=params,
            timeout=60
        )

        # Stop this attempt if the API returns an HTTP error,
        # such as 400, 404, 429, or 500
        response.raise_for_status()

        # Convert the JSON response into a Python dictionary
        weather_json = response.json()

        # Confirm that the request succeeded
        print(
            f"API request succeeded on attempt {attempt}"
        )

        # Exit the retry loop because valid data was received
        break

    except requests.exceptions.RequestException as error:
        # Display the reason the current attempt failed
        print(
            f"Attempt {attempt} failed:",
            error
        )

        # If the final attempt also fails,
        # stop the notebook and show the original error
        if attempt == max_attempts:
            raise

        # Wait five seconds before trying the API again
        print("Waiting 5 seconds before retrying...")
        time.sleep(5)


# Inspect important metadata returned by Open-Meteo
print("Timezone:", weather_json["timezone"])
print(
    "UTC offset in seconds:",
    weather_json["utc_offset_seconds"]
)


# Inspect the first five timestamps and temperatures
print(
    "First timestamps:",
    weather_json["hourly"]["time"][:5]
)

print(
    "First temperatures:",
    weather_json["hourly"]["temperature_2m"][:5]
)


# Import the Spark function used to convert strings into timestamps
from pyspark.sql.functions import to_timestamp


# Extract the hourly section from the API response
hourly_data = weather_json["hourly"]


# Pair each timestamp with its corresponding temperature
weather_rows = list(
    zip(
        hourly_data["time"],
        hourly_data["temperature_2m"]
    )
)


# Create a Spark DataFrame from the hourly weather records
bronze_weather_df = spark.createDataFrame(
    weather_rows,
    [
        "weather_timestamp",
        "temperature_celsius"
    ]
)


# Convert the timestamp column from string format
# into Spark's timestamp data type
bronze_weather_df = bronze_weather_df.withColumn(
    "weather_timestamp",
    to_timestamp("weather_timestamp")
)


# Display the Bronze DataFrame in chronological order
display(
    bronze_weather_df.orderBy(
        "weather_timestamp"
    )
)


# Inspect the DataFrame schema
bronze_weather_df.printSchema()


# Save the DataFrame as a managed Bronze Delta table
#
# overwrite is appropriate for this fixed historical rebuild.
# Later, automation may use append or MERGE instead.
(
    bronze_weather_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.default.bronze_weather_hourly"
    )
)


# Read the saved Bronze Delta table back into Spark
saved_bronze_weather_df = spark.table(
    "workspace.default.bronze_weather_hourly"
)


# Verify the saved Bronze records in chronological order
display(
    saved_bronze_weather_df.orderBy(
        "weather_timestamp"
    )
)


# Confirm the number of rows stored in Bronze
print(
    "Bronze rows:",
    saved_bronze_weather_df.count()
)
