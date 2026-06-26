"""
Weather Pipeline DAG
====================
Orchestrates the daily weather data pipeline using a Medallion architecture:

    Task 1 (ingest_to_gcs)     → Open-Meteo API → GCS JSON          (Bronze)
    Task 2 (gcs_to_bigquery)   → GCS JSON → BigQuery raw table      (Bronze in BQ)
    Task 3 (dbt_run_staging)   → dbt staging models → Silver table   (cleaned)
    Task 4 (dbt_test)          → dbt tests validate Silver quality
    Task 5 (dbt_run_marts)     → dbt mart models → Gold table        (aggregated)
    Task 6 (dbt_test_marts)    → dbt tests validate Gold quality

Schedule: @daily (midnight UTC = 5:30 AM IST)
Cities:   Hyderabad, Bangalore, Mumbai
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Add scripts directory to Python path so we can import ingest_weather
sys.path.insert(0, "/opt/airflow/scripts")

from ingest_weather import CITIES, ingest_weather_data  # noqa: E402

from google.cloud import bigquery, storage  # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "gen-lang-client-0461437803")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "de-p1-weather-bronze")
BQ_RAW_DATASET = "weather_raw"
BQ_RAW_TABLE = f"{GCP_PROJECT}.{BQ_RAW_DATASET}.raw_weather_hourly"

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task 2: Load GCS → BigQuery (Bronze in BQ)
# ---------------------------------------------------------------------------
def load_gcs_to_bigquery(**kwargs):
    """
    Read raw JSON files from GCS for the execution date,
    flatten the hourly arrays, and insert rows into BigQuery.
    """
    logical_date = kwargs["ds"]  # YYYY-MM-DD

    storage_client = storage.Client()
    bq_client = bigquery.Client(project=GCP_PROJECT)

    # Ensure dataset exists
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT, BQ_RAW_DATASET)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        bq_client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset: {BQ_RAW_DATASET}")

    # Define table schema
    schema = [
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT"),
        bigquery.SchemaField("longitude", "FLOAT"),
        bigquery.SchemaField("time", "STRING"),
        bigquery.SchemaField("temperature_2m", "FLOAT"),
        bigquery.SchemaField("relative_humidity_2m", "FLOAT"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("source_file", "STRING"),
    ]

    table_ref = bigquery.TableReference(dataset_ref, "raw_weather_hourly")
    try:
        bq_client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table, exists_ok=True)
        print(f"Created table: {BQ_RAW_TABLE}")

    # Process each city's JSON files from GCS
    bucket = storage_client.bucket(GCS_BUCKET)
    rows_to_insert = []

    for city in CITIES:
        prefix = f"weather/raw/{city['name']}/{logical_date}/"
        blobs = list(bucket.list_blobs(prefix=prefix))

        if not blobs:
            print(f"⚠ No files found for {city['name']} on {logical_date}")
            continue

        for blob in blobs:
            print(f"Processing: gs://{GCS_BUCKET}/{blob.name}")
            content = blob.download_as_text()
            data = json.loads(content)

            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            humidities = hourly.get("relative_humidity_2m", [])

            ingested_at = datetime.now(timezone.utc).isoformat()

            for i in range(len(times)):
                rows_to_insert.append({
                    "city": city["name"],
                    "latitude": data.get("latitude", city["latitude"]),
                    "longitude": data.get("longitude", city["longitude"]),
                    "time": times[i],
                    "temperature_2m": temps[i] if i < len(temps) else None,
                    "relative_humidity_2m": humidities[i] if i < len(humidities) else None,
                    "ingested_at": ingested_at,
                    "source_file": f"gs://{GCS_BUCKET}/{blob.name}",
                })

    if rows_to_insert:
        errors = bq_client.insert_rows_json(BQ_RAW_TABLE, rows_to_insert)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
        print(f"✓ Inserted {len(rows_to_insert)} rows into {BQ_RAW_TABLE}")
    else:
        print("⚠ No rows to insert")


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="Daily weather pipeline: Open-Meteo → GCS → BigQuery → dbt (Silver/Gold)",
    schedule="@daily",
    start_date=datetime(2026, 6, 25),
    catchup=False,
    tags=["weather", "data-engineering", "medallion"],
) as dag:

    # Task 1: Ingest from Open-Meteo API → GCS (Bronze)
    task_ingest_to_gcs = PythonOperator(
        task_id="ingest_to_gcs",
        python_callable=ingest_weather_data,
    )

    # Task 2: Load from GCS → BigQuery raw table (Bronze in BQ)
    task_gcs_to_bigquery = PythonOperator(
        task_id="gcs_to_bigquery",
        python_callable=load_gcs_to_bigquery,
    )

    # Task 3: dbt run staging → Silver table (cleaned, typed)
    task_dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select staging"
        ),
    )

    # Task 4: dbt test → validate Silver data quality
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --select staging"
        ),
    )

    # Task 5: dbt run marts → Gold table (daily aggregates, BI-ready)
    task_dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select marts"
        ),
    )

    # Task 6: dbt test marts → validate Gold data quality
    task_dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --select marts"
        ),
    )

    # Linear dependency chain
    (
        task_ingest_to_gcs
        >> task_gcs_to_bigquery
        >> task_dbt_staging
        >> task_dbt_test
        >> task_dbt_marts
        >> task_dbt_test_marts
    )
