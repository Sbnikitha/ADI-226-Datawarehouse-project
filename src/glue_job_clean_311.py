import json
from datetime import datetime
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession

# --------------------------------
# Static config (no job arguments)
# --------------------------------
job_name = "glue_job_clean_311"
input_path = [
    "s3://da226-nyc-data-warehouse/raw/311-complaints/complaints_pull_311_2024.json",
    "s3://da226-nyc-data-warehouse/raw/311-complaints/complaints_pull_311_2025.json"
]

# 🚀 Final Parquet output for Redshift COPY
output_base = "s3://da226-nyc-data-warehouse/staging/311-complaints-cleaned/airflow_single_output/"

precision = 7
min_lat, max_lat = 40.4, 41.0
min_lon, max_lon = -74.3, -73.7

# -------------------
# Spark/Glue session
# -------------------
sc = SparkContext.getOrCreate()
sc.setLogLevel("WARN")
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, {})

# ---------------------------------
# Helper: robust timestamp parsing
# ---------------------------------
def parse_timestamp(col):
    return F.coalesce(
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(col, "MM/dd/yyyy hh:mm:ss a"),
        F.to_timestamp(col, "M/d/yyyy h:mm:ss a"),
    )

# ------------------------
# Helper: geohash (Python)
# ------------------------
def _encode_geohash(lat, lon, precision=7):
    if lat is None or lon is None:
        return None
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    geohash = []
    bits = 0
    bit = 0
    ch = 0
    base32 = '0123456789bcdefghjkmnpqrstuvwxyz'

    while len(geohash) < int(precision):
        if bit % 2 == 0:
            mid = (lon_range[0] + lon_range[1]) / 2
            if lon > mid:
                ch |= (1 << (4 - bits))
                lon_range[0] = mid
            else:
                lon_range[1] = mid
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat > mid:
                ch |= (1 << (4 - bits))
                lat_range[0] = mid
            else:
                lat_range[1] = mid
        bits += 1
        if bits == 5:
            geohash.append(base32[ch])
            bits = 0
            ch = 0
        bit += 1

    return ''.join(geohash)

encode_geohash_udf = F.udf(lambda la, lo: _encode_geohash(la, lo, precision), T.StringType())

# --------------
# Read input
# --------------
print("📥 Reading raw input from:")
for p in input_path:
    print(" -", p)

try:
    df = spark.read.option("multiLine", "true").json(input_path)
except:
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# Ensure required columns exist
expected_cols = [
    "unique_key","created_date","closed_date","agency","agency_name","complaint_type",
    "descriptor","location_type","incident_zip","incident_address","street_name",
    "cross_street_1","cross_street_2","intersection_street_1","intersection_street_2",
    "address_type","status","resolution_description","resolution_action_updated_date",
    "community_board","borough","x_coordinate_state_plane","y_coordinate_state_plane",
    "open_data_channel_type","park_facility_name","park_borough","latitude","longitude"
]
for c in expected_cols:
    if c not in df.columns:
        df = df.withColumn(c, F.lit(None).cast("string"))

# ------------------------------
# Core cleaning & enrichment
# ------------------------------
clean = (
    df
    .withColumn("created_ts_local", parse_timestamp(F.col("created_date")))
    .withColumn("closed_ts_local", parse_timestamp(F.col("closed_date")))
    .withColumn("created_ts", F.to_utc_timestamp(F.col("created_ts_local"), "America/New_York"))
    .withColumn("closed_ts", F.to_utc_timestamp(F.col("closed_ts_local"), "America/New_York"))
    .withColumn("latitude", F.col("latitude").cast(T.DoubleType()))
    .withColumn("longitude", F.col("longitude").cast(T.DoubleType()))
    .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    .filter((F.col("latitude") >= min_lat) & (F.col("latitude") <= max_lat))
    .filter((F.col("longitude") >= min_lon) & (F.col("longitude") <= max_lon))
    .withColumn("borough", F.upper(F.trim(F.col("borough"))))
    .withColumn("complaint_type", F.trim(F.col("complaint_type")))
    .withColumn("agency", F.trim(F.col("agency")))
    .withColumn("created_year", F.year(F.col("created_ts")))
    .dropDuplicates(["unique_key"])
)

# Duration hours
clean = clean.withColumn(
    "duration_hours",
    F.when(F.col("closed_ts").isNotNull(),
           (F.col("closed_ts").cast("long") - F.col("created_ts").cast("long")) / 3600.0)
)

# Geohash
clean = clean.withColumn("geohash", encode_geohash_udf(F.col("latitude"), F.col("longitude")))

final_df = clean

# ------------------------------
# WRITE PARQUET OUTPUT
# ------------------------------
print("📤 Writing PARQUET output for Redshift...")

(
    final_df
    .coalesce(1)  # Single file for Redshift COPY
    .write
    .mode("overwrite")
    .parquet(output_base)
)

print("\n✅ Done. Cleaned 311 complaints written as PARQUET:")
print(json.dumps({"parquet_output": output_base}, indent=2))

job.commit()
