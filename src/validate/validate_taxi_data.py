# src/validate/validate_taxi_data.py

import os
import pandas as pd
import boto3
import psycopg2
from io import BytesIO
from dotenv import load_dotenv

load_dotenv(dotenv_path="docker/.env")


def get_db_connection():
    """Returns a psycopg2 connection to the warehouse Postgres DB."""
    return psycopg2.connect(
        host="postgres-warehouse",
        port=5432,  # our mapped host port for postgres-warehouse
        dbname=os.getenv("WAREHOUSE_DB_NAME"),
        user=os.getenv("WAREHOUSE_DB_USER"),           
        password=os.getenv("WAREHOUSE_DB_PASSWORD")
    )


def validate_taxi_data(year: int, month: int) -> dict:
    """
    Reads the raw Parquet file from S3, validates it, writes good/bad rows
    to processed/ and rejected/ respectively, and updates pipeline_run_log
    with the real row_count. Returns a summary dict.
    """
    bucket = os.getenv("S3_BUCKET_NAME")
    s3 = boto3.client("s3")

    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    raw_key = f"raw/nyc_taxi/yellow/{year}/{month:02d}/{filename}"

    # Download the Parquet file from S3 into memory (not local disk)
    response = s3.get_object(Bucket=bucket, Key=raw_key)
    file_content = response['Body'].read()

    # Load it into a pandas DataFrame
    df = pd.read_parquet(BytesIO(file_content), engine="pyarrow")

    # Build a boolean mask identifying "bad" rows
    mask = (
        (df["fare_amount"] < 0) |
        (df["passenger_count"] == 0) |
        (df["tpep_pickup_datetime"] > df["tpep_dropoff_datetime"])
    )

    # Split into good_df and bad_df using the mask
    bad_df = df[mask]
    good_df = df[~mask]

    total_rows = len(df)
    good_rows = len(good_df)
    bad_rows = len(bad_df)

    # Write good_df to processed/, bad_df to rejected/
    processed_key = f"processed/nyc_taxi/yellow/{year}/{month:02d}/{filename}"
    rejected_key = f"rejected/nyc_taxi/yellow/{year}/{month:02d}/{filename}"

    good_buffer = BytesIO()
    good_df.to_parquet(good_buffer, engine="pyarrow", index=False)
    good_buffer.seek(0)
    s3.upload_fileobj(good_buffer, bucket, processed_key)

    bad_buffer = BytesIO()
    bad_df.to_parquet(bad_buffer, engine="pyarrow", index=False)
    bad_buffer.seek(0)  # rewind cursor to start before reading for upload
    s3.upload_fileobj(bad_buffer, bucket, rejected_key)

    # Update pipeline_run_log with the real row_count
    conn = get_db_connection()
    cursor = conn.cursor()
    update_sql = (
        "UPDATE pipeline_run_log "
        "SET row_count = %s "
        "WHERE processed_file_name = %s AND status = 'SUCCESS';"
    )
    cursor.execute(update_sql, (total_rows, filename))
    conn.commit()
    cursor.close()
    conn.close()

    # Return a summary dict
    return {
        "filename": filename,
        "raw_s3_key": raw_key,
        "total_rows": total_rows,
        "good_rows": good_rows,
        "bad_rows": bad_rows,
        "bad_row_pct": round((bad_rows / total_rows) * 100, 2) if total_rows > 0 else 0
    }


if __name__ == "__main__":
    result = validate_taxi_data(2024, 3)
    print(result)