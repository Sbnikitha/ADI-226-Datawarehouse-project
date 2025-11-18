import json
import boto3
import awswrangler as wr
import os
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        s3_bucket = os.environ['S3_BUCKET']
        input_key = os.environ.get('INPUT_KEY', 'raw/airbnb/combined_listings_2024.csv')
        
        if 'input_key' in event:
            input_key = event['input_key']
        
        print(f"Reading from s3://{s3_bucket}/{input_key}")
        
        # Read CSV
        s3_path = f's3://{s3_bucket}/{input_key}'
        df = wr.s3.read_csv(path=s3_path)
        
        print(f"✓ Read {len(df)} records")
        print(f"✓ Columns: {list(df.columns)[:10]}")
        
        # Save as JSON
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_key = f'raw/airbnb/listings_{timestamp}.json'
        
        data_json = df.to_json(orient='records')
        s3.put_object(
            Bucket=s3_bucket,
            Key=output_key,
            Body=data_json,
            ContentType='application/json'
        )
        
        print(f"✓ Saved to s3://{s3_bucket}/{output_key}")
        
        return {
            'statusCode': 200,
            'body': {
                'records_read': len(df),
                'columns': list(df.columns),
                's3_key': output_key,
                'timestamp': timestamp
            }
        }
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e