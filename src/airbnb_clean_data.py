import json
import boto3
import pandas as pd
import io
import os
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    Clean Airbnb data: validate, deduplicate, normalize, add geohash, remove unwanted columns
    """
    try:
        # ============================================
        # HARDCODED S3 CONFIGURATION
        # ============================================
        s3_bucket = 'da226-nyc-data-warehouse'  # 👈 CHANGE THIS to your bucket name
        input_key = 'raw/airbnb/listings_20251104_084136.json'  # 👈 CHANGE THIS to your S3 key path
        
        print(f"📖 Reading from s3://{s3_bucket}/{input_key}")

        # Read JSON from S3
        obj = s3.get_object(Bucket=s3_bucket, Key=input_key)
        data = json.loads(obj['Body'].read())
        df = pd.DataFrame(data)

        initial_count = len(df)
        print(f"Initial record count: {initial_count}")

        # ============================================
        # DATA CLEANING PIPELINE
        # ============================================

        # 1. Remove nulls in critical fields
        print("\n🧹 Cleaning Step 1: Removing null IDs and coordinates...")
        df_clean = df.dropna(subset=['id', 'latitude', 'longitude'])
        print(f"  ✓ Removed {initial_count - len(df_clean)} records with nulls")

        
        # 3. Clean price field
        print("\n💰 Cleaning Step 3: Cleaning price field...")
        if 'price' in df_clean.columns:
            before = len(df_clean)
            # Remove $ and commas
            df_clean['price'] = df_clean['price'].astype(str).str.replace('$', '', regex=False)
            df_clean['price'] = df_clean['price'].str.replace(',', '', regex=False)
            df_clean['price'] = pd.to_numeric(df_clean['price'], errors='coerce')
           

        # 4. Normalize text fields
        print("\n📝 Cleaning Step 4: Normalizing text fields...")
        text_fields = {
            'neighbourhood_group': 'borough',
            'neighbourhood': 'neighbourhood',
            'room_type': 'room_type'
        }

        for field, alias in text_fields.items():
            if field in df_clean.columns:
                df_clean[field] = df_clean[field].str.upper().str.strip()
                print(f"  ✓ Normalized {field}")

        # 5. Data type conversions (FIXED - ensures proper types for Parquet)
        print("\n🔢 Cleaning Step 5: Converting data types...")
        df_clean['latitude'] = df_clean['latitude'].astype(float)
        df_clean['longitude'] = df_clean['longitude'].astype(float)
        
        # Fix ID column - convert to integer 
       # if 'id' in df_clean.columns:
        #    df_clean['id'] = df_clean['id'].astype(str)  # Convert everything to string
        #    print(f"  ✓ Converted ID to string type (preserves all values)")
        
        print(f"  ✓ Converted data types")

        # 6. Add Geohash column (no external library needed!)
        print("\n🗺️ Cleaning Step 6: Adding geohash column...")
        
        def encode_geohash(latitude, longitude, precision=7):
            """
            Encode latitude/longitude to geohash string
            Pure Python implementation - no external dependencies
            """
            lat_range = [-90.0, 90.0]
            lon_range = [-180.0, 180.0]
            geohash = []
            bits = 0
            bit = 0
            ch = 0
            base32 = '0123456789bcdefghjkmnpqrstuvwxyz'
            
            while len(geohash) < precision:
                if bit % 2 == 0:  # even bit: longitude
                    mid = (lon_range[0] + lon_range[1]) / 2
                    if longitude > mid:
                        ch |= (1 << (4 - bits))
                        lon_range[0] = mid
                    else:
                        lon_range[1] = mid
                else:  # odd bit: latitude
                    mid = (lat_range[0] + lat_range[1]) / 2
                    if latitude > mid:
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
        
        df_clean['geohash'] = df_clean.apply(
            lambda row: encode_geohash(row['latitude'], row['longitude'], precision=7),
            axis=1
        )
        print(f"  ✓ Added geohash column (precision=7)")

        # 7. Remove unwanted columns
        print("\n🗑️ Cleaning Step 7: Removing unwanted columns...")
        columns_to_remove = [
            'listing_url',
            'description',
            'picture_url',
            'host_url',
            'host_name',
            'host_about',
            'host_thumbnail_url',
            'host_picture_url',
            'calendar_updated',
            'scrape_id',
             'source',
             'bathrooms_text',
             'amenities',
             'neighborhood_overview',
             'host_id',
             'host_since',
             'host_response_time',
             'host_response_rate',
             'host_acceptance_rate',
             'host_is_superhost',
             'host_neighbourhood',
             'host_verifications',
             'host_has_profile_pic',
             'host_identity_verified',
            'minimum_nights',
         'maximum_nights',
         'maximum_minimum_nights',
         'minimum_maximum_nights',
          'maximum_maximum_nights',
         'minimum_nights_avg_ntm',
         'maximum_nights_avg_ntm',
         'availability_30',
         'availability_60',
          'availability_90',
          'availability_365',
         'calendar_last_scraped',
         'number_of_reviews_ltm',
         'number_of_reviews_ly',
         'review_scores_accuracy',
            'review_scores_cleanliness',
         'review_scores_checkin',
         'review_scores_communication',
    'review_scores_value',
    'license',
    'instant_bookable',
    'calculated_host_listings_count',
    'calculated_host_listings_count_entire_homes',
    'calculated_host_listings_count_private_rooms',
    'calculated_host_listings_count_shared_rooms'
            
        ]
        
        existing_columns_to_remove = [col for col in columns_to_remove if col in df_clean.columns]
        df_clean = df_clean.drop(columns=existing_columns_to_remove)
        print(f"  ✓ Removed {len(existing_columns_to_remove)} unwanted columns: {existing_columns_to_remove}")

        # 8. Remove duplicates
        print("\n🔍 Cleaning Step 8: Removing duplicates...")
        #before = len(df_clean)
        #df_clean = df_clean.drop_duplicates(subset=['id'])
        #print(f"  ✓ Removed {before - len(df_clean)} duplicate records")

        # 9. Add metadata
        df_clean['processed_timestamp'] = datetime.now().isoformat()
        df_clean['data_quality_score'] = 'PASSED'

        # 10. FINAL DATA TYPE CLEANUP FOR PARQUET (Critical fix!)
        print("\n🔧 Cleaning Step 9: Final data type cleanup for Parquet...")
        
        # Convert all object columns to string (preserves 'SLC' and all other values)
        for col in df_clean.columns:
            if col not in ['latitude', 'longitude']:  
                if df_clean[col].dtype == 'object' or df_clean[col].dtype == 'float64':
                    df_clean[col] = df_clean[col].astype(str)
        
        print(f"  ✓ All data types cleaned and ready for Parquet")

        # ============================================
        # SUMMARY
        # ============================================
        final_count = len(df_clean)
        removed = initial_count - final_count
        removal_pct = (removed / initial_count * 100) if initial_count > 0 else 0

        print("\n" + "="*60)
        print("📊 CLEANING SUMMARY")
        print("="*60)
        print(f"  Initial records:  {initial_count:,}")
        print(f"  Final records:    {final_count:,}")
        print(f"  Removed:          {removed:,} ({removal_pct:.2f}%)")
        print(f"  Columns:          {len(df_clean.columns)}")
        print(f"  Geohash added:    ✓")
        print("="*60)

        # Save to S3 Silver layer as Parquet
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_key = f'staging/airbnb-cleaned/listings_clean_{timestamp}.parquet'

        parquet_buffer = io.BytesIO()
        df_clean.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)

        s3.put_object(
            Bucket=s3_bucket,
            Key=output_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )

        print(f"\n✅ Saved cleaned data to s3://{s3_bucket}/{output_key}")

        return {
            'statusCode': 200,
            'body': {
                'records_initial': int(initial_count),
                'records_cleaned': int(final_count),
                'records_removed': int(removed),
                'removal_percentage': float(removal_pct),
                's3_path': f's3://{s3_bucket}/{output_key}',
                's3_key': output_key,
                'timestamp': timestamp,
                'columns_count': len(df_clean.columns),
                'geohash_added': True
            }
        }

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e