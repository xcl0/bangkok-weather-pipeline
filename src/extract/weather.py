import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry


# Create a session that caches identical requests for one hour
cache_session = requests_cache.CachedSession(
    ".cache",
    expire_after=3600
)

# Retry the request if a temporary error occurs
retry_session = retry(
    cache_session,
    retries=5,
    backoff_factor=0.2
)

# Create the Open-Meteo client
openmeteo = openmeteo_requests.Client(session=retry_session)


url = "https://api.open-meteo.com/v1/forecast"

params = {
    "latitude": 13.7563,
    "longitude": 100.5018,
    "hourly": "temperature_2m",
    "timezone": "Asia/Bangkok",
}

responses = openmeteo.weather_api(url, params=params)

# We requested one location, so take the first response
response = responses[0]

print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation: {response.Elevation()} metres")
print(f"Timezone: {response.Timezone().decode()}")


# Extract the hourly section
hourly = response.Hourly()

# Variable 0 means the first variable requested:
# temperature_2m
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()


# Construct one timestamp for every hourly temperature
hourly_data = {
    "date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    ).tz_convert(response.Timezone().decode())
}

hourly_data["temperature_2m"] = hourly_temperature_2m


# Convert the dictionary into a table
hourly_dataframe = pd.DataFrame(hourly_data)

print("\nHourly data:")
print(hourly_dataframe)
