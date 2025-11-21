from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator
from airflow.providers.amazon.aws.sensors.step_function import StepFunctionExecutionSensor
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import psycopg2
import json


# ============================================================
# CONFIGURATION
# ============================================================

AWS_CONN_ID = "aws_default"

REDSHIFT_CONN = {
    "host": "airflow-demo.023381192327.us-east-1.redshift-serverless.amazonaws.com",
    "port": 5439,
    "dbname": "dev",
    "user": "admin",
    "password": "DYKVRbjxfw767+",
}

# Output S3 staging data
S3_311_CLEANED_PATH = "s3://da226-nyc-data-warehouse/staging/311-complaints-cleaned/single_output"

# IAM role for Redshift COPY
IAM_ROLE = "arn:aws:iam::023381192327:role/RedshiftS3ReadRole"

# Glue job name
GLUE_311_JOB = "glue_job_clean_311"

# Step Function for Airbnb ETL
STEP_FUNCTION_ARN = (
    "arn:aws:states:us-east-1:023381192327:stateMachine:da226-etl-pipeline"
)


# ============================================================
# LOAD DIM_DATE (shared dim for Airbnb + 311)
# ============================================================

def load_dim_date():
    conn = psycopg2.connect(
        dbname=REDSHIFT_CONN["dbname"],
        user=REDSHIFT_CONN["user"],
        password=REDSHIFT_CONN["password"],
        host=REDSHIFT_CONN["host"],
        port=REDSHIFT_CONN["port"],
    )
    cur = conn.cursor()

    print("📅 Loading dim_date (incremental)...")

    sql = """
    WITH all_dates AS (
        SELECT created_ts::date AS dt
        FROM staging.staging_311
        WHERE created_ts IS NOT NULL

        UNION

        SELECT closed_ts::date AS dt
        FROM staging.staging_311
        WHERE closed_ts IS NOT NULL

        UNION

        SELECT last_scraped::date AS dt
        FROM staging.staging_airbnb
        WHERE last_scraped IS NOT NULL

        UNION

        SELECT host_since::date AS dt
        FROM staging.staging_airbnb
        WHERE host_since IS NOT NULL
    )
    INSERT INTO public.dim_date (
        date_id, date_value, year, quarter, month, month_name,
        day, day_of_week, day_name, is_weekend
    )
    SELECT DISTINCT
        TO_CHAR(dt, 'YYYYMMDD')::INT AS date_id,
        dt AS date_value,
        EXTRACT(YEAR FROM dt) AS year,
        EXTRACT(QUARTER FROM dt) AS quarter,
        EXTRACT(MONTH FROM dt) AS month,
        TO_CHAR(dt, 'Month') AS month_name,
        EXTRACT(DAY FROM dt) AS day,
        EXTRACT(DOW FROM dt) AS day_of_week,
        TO_CHAR(dt, 'Day') AS day_name,
        CASE WHEN EXTRACT(DOW FROM dt) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
    FROM all_dates
    WHERE dt IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM public.dim_date d 
        WHERE d.date_id = TO_CHAR(dt,'YYYYMMDD')::INT
    );
    """

    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

    print("✅ dim_date load complete.")


# ============================================================
# LOAD 311 INTO REDSHIFT (INCREMENTAL)
# ============================================================

def load_311_to_redshift():
    conn = psycopg2.connect(
        dbname=REDSHIFT_CONN["dbname"],
        user=REDSHIFT_CONN["user"],
        password=REDSHIFT_CONN["password"],
        host=REDSHIFT_CONN["host"],
        port=REDSHIFT_CONN["port"],
    )
    cur = conn.cursor()

    print("🧹 Clearing staging.staging_311")
    cur.execute("TRUNCATE TABLE staging.staging_311;")

    print("📥 COPY staging.staging_311 FROM S3 PARQUET")
    cur.execute(
        f"""
        COPY staging.staging_311
        FROM '{S3_311_CLEANED_PATH}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET;
        """
    )

    print("📌 Updating dim tables...")

    cur.execute("""
        INSERT INTO dim.dim_311_agency (agency_name)
        SELECT DISTINCT TRIM(s.agency)
        FROM staging.staging_311 s
        LEFT JOIN dim.dim_311_agency a ON a.agency_name = TRIM(s.agency)
        WHERE s.agency IS NOT NULL
          AND a.agency_key IS NULL;
    """)

    cur.execute("""
        INSERT INTO dim.dim_311_borough (borough_name)
        SELECT DISTINCT TRIM(UPPER(s.borough))
        FROM staging.staging_311 s
        LEFT JOIN dim.dim_311_borough b ON b.borough_name = TRIM(UPPER(s.borough))
        WHERE s.borough IS NOT NULL
          AND b.borough_key IS NULL;
    """)

    cur.execute("""
        INSERT INTO dim.dim_311_location (geohash, latitude, longitude, city, borough)
        SELECT DISTINCT
          s.geohash,
          s.latitude,
          s.longitude,
          NULLIF(TRIM(s.city), '') AS city,
          NULLIF(UPPER(TRIM(s.borough)), '') AS borough
        FROM staging.staging_311 s
        LEFT JOIN dim.dim_311_location d ON d.geohash = s.geohash
        WHERE s.geohash IS NOT NULL
          AND d.location_key IS NULL;
    """)

    cur.execute("""
        INSERT INTO dim.dim_311_complaint (complaint_type_name)
        SELECT DISTINCT TRIM(s.complaint_type)
        FROM staging.staging_311 s
        LEFT JOIN dim.dim_311_complaint c ON c.complaint_type_name = TRIM(s.complaint_type)
        WHERE s.complaint_type IS NOT NULL
          AND c.complaint_type_key IS NULL;
    """)

    print("📌 Loading public.fact_311_complaint")

    cur.execute("""
        INSERT INTO public.fact_311_complaint (
            complaint_id,
            geohash,
            created_date_key,
            closed_date_key,
            agency_key,
            borough_key,
            location_key,
            complaint_type_key,
            duration_hours,
            latitude,
            longitude
        )
        SELECT
            CASE WHEN s.unique_key ~ '^[0-9]+$' THEN s.unique_key::BIGINT END AS complaint_id,
            s.geohash,
            CASE WHEN s.created_ts IS NOT NULL
                 THEN TO_CHAR(s.created_ts, 'YYYYMMDD')::INT END AS created_date_key,
            CASE WHEN s.closed_ts IS NOT NULL
                 THEN TO_CHAR(s.closed_ts, 'YYYYMMDD')::INT END AS closed_date_key,
            a.agency_key,
            b.borough_key,
            d.location_key,
            c.complaint_type_key,
            s.duration_hours,
            s.latitude,
            s.longitude
        FROM staging.staging_311 s
        LEFT JOIN dim.dim_311_agency a ON a.agency_name = TRIM(s.agency)
        LEFT JOIN dim.dim_311_borough b ON b.borough_name = TRIM(UPPER(s.borough))
        LEFT JOIN dim.dim_311_location d ON d.geohash = s.geohash
        LEFT JOIN dim.dim_311_complaint c ON c.complaint_type_name = TRIM(s.complaint_type)
        LEFT JOIN public.fact_311_complaint f
               ON f.complaint_id = CASE WHEN s.unique_key ~ '^[0-9]+$' THEN s.unique_key::BIGINT END
        WHERE f.complaint_id IS NULL
          AND s.geohash IS NOT NULL
          AND s.latitude IS NOT NULL
          AND s.longitude IS NOT NULL;
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Redshift load done.")


# ============================================================
# DAG DEFINITION
# ============================================================

default_args = {
    "owner": "data_engineering",
    "retries": 0,
}

dag = DAG(
    dag_id="nyc_hybrid_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    default_args=default_args,
    tags=["311", "airbnb", "glue", "stepfunctions", "redshift"]
)


# ============================================================
# TASKS
# ============================================================

start = EmptyOperator(task_id="start", dag=dag)

#  Airbnb ETL via Step Function
trigger_airbnb = StepFunctionStartExecutionOperator(
    task_id="trigger_airbnb_etl",
    aws_conn_id=AWS_CONN_ID,
    state_machine_arn=STEP_FUNCTION_ARN,
    name="airbnb_etl_airflow_{{ ts_nodash }}",
    state_machine_input={
        "bucket": "da226-nyc-data-warehouse",
        "staging_prefix": "staging/airbnb-cleaned/",
        "processed_prefix": "processed/airbnb/",
        "upload": {
        "body": {
            "records_read": 37601,
            "columns": [
            "id",
            "listing_url",
            "scrape_id",
            "last_scraped",
            "source",
            "name",
            "description",
            "neighborhood_overview",
            "picture_url",
            "host_id",
            "host_url",
            "host_name",
            "host_since",
            "host_location",
            "host_about",
            "host_response_time",
            "host_response_rate",
            "host_acceptance_rate",
            "host_is_superhost",
            "host_thumbnail_url",
            "host_picture_url",
            "host_neighbourhood",
            "host_listings_count",
            "host_total_listings_count",
            "host_verifications",
            "host_has_profile_pic",
            "host_identity_verified",
            "neighbourhood",
            "neighbourhood_cleansed",
            "neighbourhood_group_cleansed",
            "latitude",
            "longitude",
            "property_type",
            "room_type",
            "accommodates",
            "bathrooms",
            "bathrooms_text",
            "bedrooms",
            "beds",
            "amenities",
            "price",
            "minimum_nights",
            "maximum_nights",
            "minimum_minimum_nights",
            "maximum_minimum_nights",
            "minimum_maximum_nights",
            "maximum_maximum_nights",
            "minimum_nights_avg_ntm",
            "maximum_nights_avg_ntm",
            "calendar_updated",
            "has_availability",
            "availability_30",
            "availability_60",
            "availability_90",
            "availability_365",
            "calendar_last_scraped",
            "number_of_reviews",
            "number_of_reviews_ltm",
            "number_of_reviews_l30d",
            "availability_eoy",
            "number_of_reviews_ly",
            "estimated_occupancy_l365d",
            "estimated_revenue_l365d",
            "first_review",
            "last_review",
            "review_scores_rating",
            "review_scores_accuracy",
            "review_scores_cleanliness",
            "review_scores_checkin",
            "review_scores_communication",
            "review_scores_location",
            "review_scores_value",
            "license",
            "instant_bookable",
            "calculated_host_listings_count",
            "calculated_host_listings_count_entire_homes",
            "calculated_host_listings_count_private_rooms",
            "calculated_host_listings_count_shared_rooms",
            "reviews_per_month",
            "source_file"
            ],
            "s3_key": "raw/airbnb/listings_20251110_095030.json",
            "timestamp": "20251110_095030"
        },
        "statusCode": 200
        }
    },
    dag=dag,
)

wait_airbnb = StepFunctionExecutionSensor(
    task_id="wait_for_airbnb_etl",
    aws_conn_id=AWS_CONN_ID,
    execution_arn="{{ task_instance.xcom_pull('trigger_airbnb_etl') }}",
    poke_interval=30,
    timeout=3600,
    dag=dag,
)

#  311 ETL via Glue
run_glue_311 = GlueJobOperator(
    task_id="run_glue_311",
    job_name=GLUE_311_JOB,
    aws_conn_id=AWS_CONN_ID,
    wait_for_completion=True,
    dag=dag,
)

#  SYNC POINT
sync = EmptyOperator(task_id="sync", dag=dag)

#  Load shared dim_date
load_dim_date_task = PythonOperator(
    task_id="load_dim_date",
    python_callable=load_dim_date,
    dag=dag,
)

# Load 311 dims + facts
load_redshift = PythonOperator(
    task_id="load_311_to_redshift",
    python_callable=load_311_to_redshift,
    dag=dag,
)

finish = EmptyOperator(task_id="finish", dag=dag)


# ============================================================
# DAG FLOW
# ============================================================

start >> [trigger_airbnb, run_glue_311]

trigger_airbnb >> wait_airbnb >> sync
run_glue_311 >> sync

sync >> load_dim_date_task >> load_redshift >> finish
