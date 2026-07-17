# src/extract/download_taxi_data.py

import os
import requests
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv(dotenv_path="docker/.env")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def download_to_s3(year: int, month: int) -> bool:
    """
    Downloads one month of Yellow Taxi Parquet data and streams it to S3 raw zone.
    Returns True if uploaded, False if skipped (already exists).
    """
    bucket = os.getenv("S3_BUCKET_NAME")
    s3 = boto3.client("s3")
    
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    s3_key = f"raw/nyc_taxi/yellow/{year}/{month:02d}/{filename}"
    
    # STEP 1: Check if this file already exists in S3 (idempotency check)
    try:
        s3.head_object(Bucket=bucket, Key=s3_key)
        print(f"File already exists at {s3_key}, skipping download.")
        return False
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            pass  # doesn't exist yet — continue to download
        else:
            raise  # some other error (permissions, etc.) — don't swallow it
    
    # STEP 3: Build source URL
    url = f"{BASE_URL}/{filename}"
    
    # STEP 4: Streaming HTTP request
    response = requests.get(url, stream=True)
    
    # STEP 5: Validate response succeeded
    response.raise_for_status()  # throws an exception automatically if status is 4xx/5xx
    
    # STEP 6: Upload to S3 via streaming & Log success and return True
    s3.upload_fileobj(response.raw, bucket, s3_key)
    print(f"Successfully loaded to s3://{bucket}/{s3_key}")
    return True


if __name__ == "__main__":
    download_to_s3(2024, 2)