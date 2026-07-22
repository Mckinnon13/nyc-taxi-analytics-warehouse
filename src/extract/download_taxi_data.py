# src/extract/download_taxi_data.py

import os
import uuid
import requests
import boto3
import psycopg2
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv(dotenv_path="docker/.env")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def get_db_connection():
    """Returns a psycopg2 connection to the warehouse Postgres DB."""
    return psycopg2.connect(
        host="localhost",
        port=5433,  # our mapped host port for postgres-warehouse
        dbname=os.getenv("WAREHOUSE_DB_NAME"),
        user=os.getenv("WAREHOUSE_DB_USER"),
        password=os.getenv("WAREHOUSE_DB_PASSWORD")
    )

def download_to_s3(year: int, month: int) -> bool:
    """
    Downloads one month of Yellow Taxi Parquet data and streams it to S3 raw zone.
    Returns True if uploaded, False if skipped (already exists).
    """
    bucket = os.getenv("S3_BUCKET_NAME")
    s3 = boto3.client("s3")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    s3_key = f"raw/nyc_taxi/yellow/{year}/{month:02d}/{filename}"
    pipeline_run_id = str(uuid.uuid4())  # unique ID for this run

    #Check if this file already exists in S3 (idempotency check)
    try:
        s3.head_object(Bucket=bucket, Key=s3_key)
        print(f"File already exists at {s3_key}, skipping download.")
        return False
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            pass  # doesn't exist yet — continue to download
        else:
            raise  # some other error (permissions, etc.) — don't swallow it
    
    #Insert an IN_PROGRESS row into pipeline_run_log, capture its id
    sql = (
        "INSERT INTO pipeline_run_log (pipeline_run_id, processed_file_name, year, month, status) "
        "VALUES (%s, %s, %s, %s, 'IN_PROGRESS') "
        "RETURNING id;"
    )
    cursor.execute(sql, (pipeline_run_id, filename, year, month))
    row_id = cursor.fetchone()[0]   #to get the RETURNING value
    conn.commit()   

    url = f"{BASE_URL}/{filename}"      #Build source URL
    response = requests.get(url, stream=True)       #Streaming HTTP request
    response.raise_for_status()  # throws an exception automatically if status is 4xx/5xx
    s3.upload_fileobj(response.raw, bucket, s3_key)     #Upload to S3 via streaming & Log success and return True

    #Update row to SUCCESS
    update_sql = (
        "UPDATE pipeline_run_log "
        "SET row_count = NULL, status = 'SUCCESS' "
        "WHERE id = %s;"
    )
    cursor.execute(update_sql, (row_id,))
    conn.commit()

    cursor.close()
    conn.close()
    print(f"Successfully loaded to s3://{bucket}/{s3_key}")
    return True


if __name__ == "__main__":
    download_to_s3(2024, 3)