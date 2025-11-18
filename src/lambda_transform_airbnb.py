import json
import boto3
import pandas as pd
import io
import os
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    Transform Airbnb cleaned data into dimension and fact tables
    
    Input: Cleaned Parquet from staging/airbnb-cleaned/
    Output: Dimension and fact tables in gold/airbnb/
    
    Creates:
    - dim_location (neighbourhood, lat/long, borough)
    - dim_property (property details)
    - dim_host (host information)
    - fact_airbnb_listings (main fact table)
    """
    try:
        s3_bucket = os.environ.get('S3_BUCKET', 'da226-nyc-data-warehouse')
        
        # Get input path from event or use default
        if 'body' in event and 's3_key' in event['body']:
            input_key = event['body']['s3_key']
        else:
            # Default: read from staging
            input_key = 'staging/airbnb-cleaned/listings_clean_20251110_083539.parquet'
        
        print(f"📖 Reading from s3://{s3_bucket}/{input_key}")
        
        # Read Parquet from S3
        obj = s3.get_object(Bucket=s3_bucket, Key=input_key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        
        print(f"✅ Loaded {len(df):,} records")
        print(f"📊 Columns available: {list(df.columns)}")
        
        # ==========================================
        # 1. dim_location - Geographic dimensions
        # ==========================================
        print("\n🗺️ Creating dim_location...")
        
        dim_location = df[[
            'neighbourhood',
            'neighbourhood_cleansed', 
            'neighbourhood_group_cleansed',
            'latitude',
            'longitude',
            'geohash'
        ]].copy()
        
        # Rename columns
        dim_location = dim_location.rename(columns={
            'neighbourhood_cleansed': 'neighbourhood_name',
            'neighbourhood_group_cleansed': 'borough'
        })
        
        
        # Add location_id
        dim_location = dim_location.reset_index(drop=True)
        dim_location['location_id'] = dim_location.index + 1
        
        # Reorder columns
        dim_location = dim_location[[
            'location_id', 'geohash', 'latitude', 'longitude',
            'neighbourhood', 'neighbourhood_name', 'borough'
        ]]
        
        print(f"  ✅ Created {len(dim_location):,} unique locations")
        
        # ==========================================
        # 2. dim_property - Property characteristics
        # ==========================================
        print("\n🏠 Creating dim_property...")
        
        dim_property = df[[
            'id',
            'property_type',
            'room_type',
            'accommodates',
            'bathrooms',
            'bedrooms',
            'beds'
        ]].copy()
        
        # Rename and clean
        dim_property = dim_property.rename(columns={'id': 'listing_id'})

        

        # Add property_id
        dim_property = dim_property.reset_index(drop=True)
        dim_property['property_id'] = dim_property.index + 1
        
        # Reorder columns
        dim_property = dim_property[[
            'property_id', 'listing_id', 'property_type', 'room_type',
            'accommodates', 'bathrooms', 'bedrooms', 'beds'
        ]]
        
        print(f"  ✅ Created {len(dim_property):,} property records")
        
        # ==========================================
        # 3. dim_host - Host information
        # ==========================================
        print("\n👤 Creating dim_host...")
        
        # Note: Using available columns (no host_id in your data)
        # We'll use the first listing per host_location as proxy
        dim_host = df[[
            'host_location',
            'host_listings_count',
            'host_total_listings_count'
        ]].copy()
        
        # Remove nulls and duplicates
        #dim_host = dim_host.dropna(subset=['host_location'])
        #dim_host = dim_host.drop_duplicates(subset=['host_location'])
        
        # Add host_id
        dim_host = dim_host.reset_index(drop=True)
        dim_host['host_id'] = dim_host.index + 1
        
        # Reorder columns
        dim_host = dim_host[[
            'host_id', 'host_location', 
            'host_listings_count', 'host_total_listings_count'
        ]]
        
        print(f"  ✅ Created {len(dim_host):,} host records")
        
        # ==========================================
        # 4. dim_date - Date dimension
        # ==========================================
        print("\n📅 Creating dim_date...")
        
        # Extract unique dates from reviews
        dates = []
        
        if 'first_review' in df.columns:
            dates.extend(pd.to_datetime(df['first_review'], errors='coerce').dropna().unique())
        
        if 'last_review' in df.columns:
            dates.extend(pd.to_datetime(df['last_review'], errors='coerce').dropna().unique())
        
        if 'last_scraped' in df.columns:
            dates.extend(pd.to_datetime(df['last_scraped'], errors='coerce').dropna().unique())
        
        # Create date dimension
        unique_dates = pd.Series(dates).unique()
        dim_date = pd.DataFrame({'date': pd.to_datetime(unique_dates)})
        dim_date = dim_date.sort_values('date').reset_index(drop=True)
        
        # Add date attributes
        dim_date['date_id'] = dim_date.index + 1
        dim_date['date_value'] = dim_date['date'].dt.date
        dim_date['year'] = dim_date['date'].dt.year
        dim_date['quarter'] = dim_date['date'].dt.quarter
        dim_date['month'] = dim_date['date'].dt.month
        dim_date['month_name'] = dim_date['date'].dt.month_name()
        dim_date['day'] = dim_date['date'].dt.day
        dim_date['day_of_week'] = dim_date['date'].dt.dayofweek
        dim_date['day_name'] = dim_date['date'].dt.day_name()
        dim_date['is_weekend'] = dim_date['day_of_week'].isin([5, 6])
        
        # Reorder columns
        dim_date = dim_date[[
            'date_id', 'date', 'date_value','year', 'quarter', 'month', 'month_name',
            'day', 'day_of_week', 'day_name', 'is_weekend'
        ]]
        
        print(f"  ✅ Created {len(dim_date):,} date records")
        
        # ==========================================
        # 5. fact_airbnb_listings - Main fact table
        # ==========================================
        print("\n📊 Creating fact_airbnb_listings...")
        
        fact_airbnb = df[[
            'id',
            'geohash',
            'property_type',
            'room_type',
            'price',
            'minimum_minimum_nights',
            'number_of_reviews',
            'number_of_reviews_l30d',
            'reviews_per_month',
            'review_scores_rating',
            'review_scores_location',
            'availability_eoy',
            'has_availability',
            'estimated_occupancy_l365d',
            'estimated_revenue_l365d',
            'first_review',
            'last_review',
            'last_scraped',
            'data_quality_score'
        ]].copy()
        
        # Rename columns
        fact_airbnb = fact_airbnb.rename(columns={
            'id': 'listing_id',
            'minimum_minimum_nights': 'minimum_nights',
            'last_scraped': 'scraped_date'
        })
        
        
        # Convert dates
        fact_airbnb['first_review'] = pd.to_datetime(fact_airbnb['first_review'], errors='coerce')
        fact_airbnb['last_review'] = pd.to_datetime(fact_airbnb['last_review'], errors='coerce')
        fact_airbnb['scraped_date'] = pd.to_datetime(fact_airbnb['scraped_date'], errors='coerce')
        
        # Add surrogate key
        fact_airbnb = fact_airbnb.reset_index(drop=True)
        fact_airbnb['fact_id'] = fact_airbnb.index + 1
        
        # Reorder columns - fact_id first
        cols = ['fact_id'] + [col for col in fact_airbnb.columns if col != 'fact_id']
        fact_airbnb = fact_airbnb[cols]
        
        print(f"  ✅ Created {len(fact_airbnb):,} fact records")
        
        # ==========================================
        # Save all to S3 Gold layer
        # ==========================================
        print("\n💾 Saving to S3 Gold layer...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        outputs = {
            'dim_location': dim_location,
            'dim_property': dim_property,
            'dim_host': dim_host,
            'dim_date': dim_date,
            'fact_airbnb_listings': fact_airbnb
        }
        
        s3_paths = {}
        record_counts = {}
        
        for name, df_output in outputs.items():
            output_key = f'gold/airbnb/{name}_{timestamp}.parquet'
            
            # Convert to Parquet
            parquet_buffer = io.BytesIO()
            df_output.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)
            
            # Upload to S3
            s3.put_object(
                Bucket=s3_bucket,
                Key=output_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            s3_paths[name] = f's3://{s3_bucket}/{output_key}'
            record_counts[name] = len(df_output)
            
            print(f"  ✅ {name}: {len(df_output):,} records → {output_key}")
        
        # ==========================================
        # Summary
        # ==========================================
        print("\n" + "="*60)
        print("✅ TRANSFORMATION COMPLETE!")
        print("="*60)
        print(f"📊 Created {len(outputs)} tables:")
        for name, count in record_counts.items():
            print(f"  • {name}: {count:,} records")
        print("="*60)
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Airbnb data transformed successfully',
                'tables_created': list(outputs.keys()),
                'record_counts': record_counts,
                's3_paths': s3_paths,
                'timestamp': timestamp,
                'input_records': len(df),
                'output_location': f's3://{s3_bucket}/gold/airbnb/'
            }
        }
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'Failed to transform Airbnb data'
            }
        }