"""
Weather Data Ingestion Script
=============================
Fetches hourly weather forecast data from the Open-Meteo API
for Hyderabad, Bangalore, and Mumbai, then uploads raw JSON
to Google Cloud Storage (Bronze layer).

API Timing:
    - Uses forecast_days=1 → returns today's 24 hourly readings (00:00–23:00)
    - When triggered daily at midnight UTC (5:30 AM IST):
        * Gets the forecast for that calendar day
        * Example: triggered 2026-06-26 00:00 UTC → forecast for 2026-06-26
    - Each city produces one JSON file per run
"""

import json
import os
from datetime import datetime, timezone

import requests
from google.cloud import storage

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CITIES = [
    {"name": "hyderabad", "latitude": 17.38, "longitude": 78.48},
    {"name": "bangalore", "latitude": 12.97, "longitude": 77.59},
    {"name": "mumbai",    "latitude": 19.08, "longitude": 72.88},
]

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_PARAMS = "temperature_2m,relative_humidity_2m"


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------
def fetch_weather(latitude: float, longitude: float) -> dict:
    """Fetch hourly weather forecast from Open-Meteo API for a single location."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": HOURLY_PARAMS,
        "forecast_days": 1,  # Only today's 24-hour forecast
        "timezone": "auto",
    }
    response = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def upload_to_gcs(data: dict, city_name: str, bucket_name: str, execution_date: str) -> str:
    """Upload JSON data to GCS Bronze layer, partitioned by city and date."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    blob_path = f"weather/raw/{city_name}/{execution_date}/weather_{timestamp}.json"

    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json",
    )

    print(f"  ✓ Uploaded: gs://{bucket_name}/{blob_path}")
    return f"gs://{bucket_name}/{blob_path}"


def ingest_weather_data(**kwargs) -> list[str]:
    """
    Main ingestion function — called by Airflow PythonOperator.

    Fetches weather data for all 3 cities and uploads to GCS.
    Returns list of GCS URIs for downstream tasks.

    Airflow passes `ds` (logical date as YYYY-MM-DD) automatically via **kwargs.
    """
    bucket_name = os.environ.get("GCS_BUCKET", "de-p1-weather-bronze")

    # Airflow's logical date (execution date) for partitioning
    logical_date = kwargs.get("ds", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    uploaded_files = []
    for city in CITIES:
        print(f"Fetching weather data for {city['name']} "
              f"(lat={city['latitude']}, lon={city['longitude']})...")

        data = fetch_weather(city["latitude"], city["longitude"])

        # Enrich raw data with pipeline metadata
        data["_metadata"] = {
            "city": city["name"],
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source_url": OPEN_METEO_URL,
            "execution_date": logical_date,
        }

        gcs_uri = upload_to_gcs(data, city["name"], bucket_name, logical_date)
        uploaded_files.append(gcs_uri)

    print(f"\n✓ Successfully ingested weather data for {len(CITIES)} cities")
    return uploaded_files


# ---------------------------------------------------------------------------
# Local testing
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    result = ingest_weather_data(ds=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    for uri in result:
        print(f"  {uri}")
