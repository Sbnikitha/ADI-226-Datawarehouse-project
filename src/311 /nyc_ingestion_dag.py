from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import boto3
import pandas as pd
import json
import io

# ------------------------------------------------------
# AWS S3 CONFIG
# ------------------------------------------------------
BUCKET = "da226-nyc-data-warehouse"

S3_311_2024 = "raw/311-complaints/complaints_pull_311_2024.json"
S3_311_2025 = "raw/311-complaints/complaints_pull_311_2025.json"
S3_AIRBNB_COMBINED = "raw/airbnb/combined_pull_listings_2025.csv"

s3 = boto3.client("s3")


# ------------------------------------------------------
# Utility: Check if file exists in S3
# ------------------------------------------------------
def s3_exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except:
        return False


# ------------------------------------------------------
# Pull NYC 311 complaints by year
# ------------------------------------------------------
def pull_311_year(year, s3_key, **context):
    print(f"\n🔍 Checking 311 complaints for {year}...")

    if s3_exists(s3_key):
        print(f"⏭ Skipping {year} — file already exists: s3://{BUCKET}/{s3_key}")
        return

    print(f"⬇️ Pulling NYC 311 complaints for {year}...")

    url = (
        "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
        f"?$where=created_date between '{year}-01-01T00:00:00' and "
        f"'{year}-12-31T23:59:59'&$limit=5000000"
    )

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    print(f"📦 Downloaded {len(data)} complaint records")

    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json"
    )

    print(f"✅ Uploaded → s3://{BUCKET}/{s3_key}")


# ------------------------------------------------------
# Pull Airbnb 2025 monthly datasets & combine into 1 file
# ------------------------------------------------------
AIRBNB_2025_LINKS = {
    "Jan": "https://data.insideairbnb.com/united-states/ny/new-york-city/2025-01-03/data/listings.csv.gz",
    "Feb": "https://data.insideairbnb.com/united-states/ny/new-york-city/2025-02-01/visualisations/listings.csv",
    "Mar": "https://data.insideairbnb.com/united-states/ny/new-york-city/2025-03-01/visualisations/listings.csv",
    "Apr": "https://data.insideairbnb.com/united-states/ny/new-york-city/2025-04-01/visualisations/listings.csv",
    "May": "https://data.insideairbnb.com/united-states/ny/new-york-city/2025-05-01/visualisations/listings.csv",
    "Jun": "https://data.insideairbnb.com/united-states/ny/new-york-city/2025-06-17/visualisations/listings.csv",
    "Jul": "https://data.insideairbnb.com/united-states/ny/new-york-city/2025-07-01/visualisations/listings.csv",
    "Aug": "https://data.insideairbnb.com/united-states/ny/new-york-city/2025-08-01/visualisations/listings.csv",
    "Sep": "https://data.insideairbnb.com/united-states/ny/new-york-city/2025-09-01/visualisations/listings.csv",
    "Oct": "https://data.insideairbnb.com/united-states/ny/new-york-city/2025-10-01/visualisations/listings.csv"
}


def pull_airbnb_2025(**context):
    print("🔍 Checking for existing combined Airbnb dataset...")

    if s3_exists(S3_AIRBNB_COMBINED):
        print(f"⏭ Skipping Airbnb 2025 — exists: {S3_AIRBNB_COMBINED}")
        return

    print("⬇️ Downloading Airbnb 2025 datasets...")

    combined = []

    for month, url in AIRBNB_2025_LINKS.items():
        print(f"➡️ {month}: {url}")

        response = requests.get(url)
        if response.status_code != 200:
            print(f"⚠️ Failed downloading {month} — skipping")
            continue

        if url.endswith(".gz"):
            df = pd.read_csv(io.BytesIO(response.content), compression="gzip")
        else:
            df = pd.read_csv(io.BytesIO(response.content))

        df["month"] = month
        combined.append(df)

    if not combined:
        raise ValueError("No Airbnb files downloaded — cannot continue")

    final_df = pd.concat(combined, ignore_index=True)
    print(f"📊 Final row count: {len(final_df)}")

    csv_buffer = io.StringIO()
    final_df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=BUCKET,
        Key=S3_AIRBNB_COMBINED,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

    print(f"✅ Uploaded combined Airbnb 2025 → s3://{BUCKET}/{S3_AIRBNB_COMBINED}")


# ------------------------------------------------------
# DAG DEFINITION
# ------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="nyc_ingestion_dag",
    description="Collects raw 311 (2024, 2025) + Airbnb 2025",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["311", "airbnb", "ingestion", "raw"]
)

# ------------------------------------------------------
# TASKS
# ------------------------------------------------------
pull_311_2024_task = PythonOperator(
    task_id="pull_311_2024",
    python_callable=pull_311_year,
    op_kwargs={"year": 2024, "s3_key": S3_311_2024},
    dag=dag
)

pull_311_2025_task = PythonOperator(
    task_id="pull_311_2025",
    python_callable=pull_311_year,
    op_kwargs={"year": 2025, "s3_key": S3_311_2025},
    dag=dag
)

pull_airbnb_2025_task = PythonOperator(
    task_id="pull_airbnb_2025",
    python_callable=pull_airbnb_2025,
    dag=dag
)

# PIPELINE ORDER
pull_311_2024_task >> pull_311_2025_task >> pull_airbnb_2025_task
