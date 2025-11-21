import json
from datetime import datetime
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
# -----------------------------
# Static (no job arguments)
# -----------------------------
job_name = "geohash-merge-311-airbnb"

# ✅ Update these paths if yours differ
complaints_s3 = "s3://da226-nyc-data-warehouse/staging/311-complaints-cleaned/"  
airbnb_s3     = "s3://da226-nyc-data-warehouse/staging/airbnb-cleaned/"                
output_s3     = "s3://da226-nyc-data-warehouse/curated/311_airbnb_geohash_join/"

# Geohash precision expectation
precision = 7  # both sides should be encoded with the same precision for exact matching

# -------------------
# Spark/Glue session
# -------------------
sc = SparkContext.getOrCreate()
sc.setLogLevel("WARN")
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, {})

# -------------------
# Geohash helpers
# -------------------
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
        if bit % 2 == 0:  # even bit: longitude
            mid = (lon_range[0] + lon_range[1]) / 2
            if lon > mid:
                ch |= (1 << (4 - bits))
                lon_range[0] = mid
            else:
                lon_range[1] = mid
        else:  # odd bit: latitude
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
# Read inputs
# --------------
print(f"Reading complaints from: {complaints_s3}")
complaints = spark.read.parquet(complaints_s3)

print(f"Reading airbnb from: {airbnb_s3}")
airbnb = spark.read.parquet(airbnb_s3)

# Ensure geohash columns exist and are normalized
if "geohash" not in [c.lower() for c in complaints.columns]:
    raise RuntimeError("Complaints dataset must contain a 'geohash' column. Re-run the cleaning job.")

if "geohash" not in [c.lower() for c in airbnb.columns]:
    # derive geohash from Airbnb lat/lon if missing
    if not {"latitude", "longitude"}.issubset(set([c.lower() for c in airbnb.columns])):
        raise RuntimeError("Airbnb dataset has no geohash and lacks latitude/longitude to compute it.")
    airbnb = airbnb.withColumn("geohash", encode_geohash_udf(F.col("latitude"), F.col("longitude")))

# normalize case and trim just in case
complaints = complaints.withColumn("geohash", F.lower(F.trim(F.col("geohash"))))
airbnb     = airbnb.withColumn("geohash", F.lower(F.trim(F.col("geohash"))))

# Optional: enforce the same precision by truncation (uncomment if needed)
# complaints = complaints.withColumn("geohash", F.substring("geohash", 1, precision))
# airbnb     = airbnb.withColumn("geohash", F.substring("geohash", 1, precision))

# --------------
# Select & rename to avoid column collisions
# --------------
complaints_sel = complaints.select(
    F.col("geohash"),
    F.col("unique_key").alias("complaint_unique_key"),
    F.col("created_ts").alias("complaint_created_ts"),
    F.col("closed_ts").alias("complaint_closed_ts"),
    F.col("duration_hours").alias("complaint_duration_hours"),
    F.col("complaint_type"),
    F.col("descriptor"),
    F.col("borough"),
    F.col("status").alias("complaint_status"),
    F.col("resolution_description").alias("complaint_resolution"),
    F.col("latitude").alias("complaint_lat"),
    F.col("longitude").alias("complaint_lon")
)

# Keep useful Airbnb fields and rename
airbnb_sel = airbnb.select(
    F.col("geohash"),
    F.col("id").alias("airbnb_id"),
    F.col("name").alias("airbnb_name"),
    F.col("neighbourhood_group_cleansed").alias("airbnb_neighbourhood_group"),
    F.col("neighbourhood_cleansed").alias("airbnb_neighbourhood"),
    F.col("room_type").alias("airbnb_room_type"),
    F.col("property_type").alias("airbnb_property_type"),
    F.col("price").alias("airbnb_price_raw"),
    F.col("latitude").alias("airbnb_lat"),
    F.col("longitude").alias("airbnb_lon")
)

# --------------
# Join on geohash (inner)
# --------------
joined = complaints_sel.join(airbnb_sel, on=["geohash"], how="inner")

# Optional: derived flags or counts per geohash
by_geo_counts = joined.groupBy("geohash").agg(
    F.countDistinct("complaint_unique_key").alias("n_complaints"),
    F.countDistinct("airbnb_id").alias("n_airbnbs")
)

# --------------
# Write outputs
# --------------
run_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
joined_out = f"{output_s3}joined/"
counts_out = f"{output_s3}by_geohash_counts/"
manifest_out = f"{output_s3}_manifests/{run_ts}/"

joined.coalesce(1).write.mode("overwrite").parquet(joined_out)
by_geo_counts.coalesce(1).write.mode("overwrite").json(counts_out)

manifest_df = spark.createDataFrame([
    {
        "job_name": job_name,
        "run_utc": run_ts,
        "geohash_precision": precision,
        "inputs": {"complaints": complaints_s3, "airbnb": airbnb_s3},
        "outputs": {"joined": joined_out, "by_geohash_counts": counts_out},
    }
])
manifest_df.coalesce(1).write.mode("overwrite").json(manifest_out)

print("\n✅ Geohash merge complete. Outputs:")
print(json.dumps({
    "joined": joined_out,
    "by_geohash_counts": counts_out,
    "manifest": manifest_out,
}, indent=2))

job.commit()
