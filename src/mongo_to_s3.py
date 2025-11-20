import json
import boto3
from pymongo import MongoClient
from io import BytesIO

# -----------------------------
# MongoDB connection
# -----------------------------
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["nyc311DB"]
collection = db["complaints2024"]

# -----------------------------
# AWS S3 connection
# -----------------------------
s3 = boto3.client("s3", region_name="us-east-1")

# S3 bucket and file path (make sure bucket exists)
bucket_name = "da226-nyc-data-warehouse"
s3_key = "raw/311-complaints/complaints2024_direct.json"

# -----------------------------
# Fetch data from MongoDB
# -----------------------------
cursor = collection.find({})
records = list(cursor)  # For large collections, consider batching

# Convert MongoDB documents to JSON
json_data = json.dumps(records, default=str).encode("utf-8")

# -----------------------------
# Upload directly to S3 (in-memory)
# -----------------------------
s3.upload_fileobj(BytesIO(json_data), "da226-nyc-data-warehouse", s3_key)

print(f"✅ Uploaded complaints2024 collection directly to s3://{bucket_name}/{s3_key}")
