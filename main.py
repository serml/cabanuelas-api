from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from collections import Counter

app = FastAPI(title="Weather API")

# Define allowed origins (replace with your frontend's URL)
origins = [
    "http://localhost:3000",  # Example: Your React development server
    "http://localhost", # local host
    "http://localhost:8000",
    "http://192.168.1.150:3033",
    "http://frontend:3000",
    "https://cuandomecaso.chavinvan.com",  # Example: Your production domain
]

# Add CORSMiddleware to allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods
    allow_headers=["*"],  # Allows all headers
)

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


def fetch_weather_data(latitude: float, longitude: float):
    """
    Fetches weather data from Open-Meteo API and returns a Pandas DataFrame.
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": "1940-01-01",
        "end_date": "2024-12-31",
        "daily": "weather_code"
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()

    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        ),
        "weather_code": daily_weather_code
    }

    daily_dataframe = pd.DataFrame(data=daily_data)
    daily_dataframe["day"] = daily_dataframe["date"].dt.day
    daily_dataframe["month"] = daily_dataframe["date"].dt.month

    return daily_dataframe


def map_weather_code_to_condition(weather_code):
    """Maps weather codes to weather conditions."""
    if weather_code in [0, 1, 2]:
        return "sunny"
    elif weather_code in [3, 45, 48]:
        return "cloudy"
    elif weather_code in [51, 53, 55, 80, 81, 82, 61, 63, 65, 56, 57, 66, 67, 95, 96, 99]:
        return "rainy"
    elif weather_code in [71, 73, 75, 77, 85, 86]:
        return "snowy"
    else:
        return "unknown"


def aggregate_weather_data(dataframe: pd.DataFrame):
    """Aggregates weather data by month and day, counting weather condition occurrences."""
    dataframe["weather_condition"] = dataframe["weather_code"].apply(map_weather_code_to_condition)

    # Group by month, then day, and count occurrences of each weather condition
    grouped = dataframe.groupby(['month', 'day'])['weather_condition'].apply(list).reset_index()

    # Count occurrences for each weather condition
    grouped['weather_counts'] = grouped['weather_condition'].apply(Counter)

    # Convert the 'weather_counts' column to a dictionary format for each row
    grouped['weather_counts'] = grouped['weather_counts'].apply(dict)

    # Remove unnecessary column
    grouped.drop(columns=['weather_condition'], inplace=True)

    return grouped.to_dict(orient="records")  # Use records orientation


@app.get("/weather/")
async def get_weather(latitude: float = Query(..., title="Latitude"), longitude: float = Query(..., title="Longitude")):
    """
    Fetches weather data for a given latitude and longitude and returns aggregated weather condition counts.
    """
    try:
        daily_dataframe = fetch_weather_data(latitude, longitude)
        aggregated_data = aggregate_weather_data(daily_dataframe)
        return aggregated_data
    except Exception as e:
        return {"error": str(e)}
