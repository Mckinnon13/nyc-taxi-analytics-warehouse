# src/load/load_to_warehouse.py

import os
import pandas as pd
import boto3
import psycopg2
from io import BytesIO
from dotenv import load_dotenv

load_dotenv(dotenv_path="docker/.env")


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_DB_HOST"),
        port=os.getenv("WAREHOUSE_DB_PORT"),
        dbname=os.getenv("WAREHOUSE_DB_NAME"),
        user=os.getenv("WAREHOUSE_DB_USER"),
        password=os.getenv("WAREHOUSE_DB_PASSWORD")
    )


def get_or_create(cursor, table, lookup_col, value, return_col):
    """Look up a dimension value; insert if not exists. Returns the surrogate key."""
    cursor.execute(f"SELECT {return_col} FROM {table} WHERE {lookup_col} = %s", (value,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        f"INSERT INTO {table} ({lookup_col}) VALUES (%s) RETURNING {return_col}",
        (value,)
    )
    return cursor.fetchone()[0]


def get_or_create_date(cursor, dt):
    """Look up or insert a date into dim_date. Returns date_id."""
    date_val = dt.date()
    cursor.execute("SELECT date_id FROM dim_date WHERE full_date = %s", (date_val,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("""
        INSERT INTO dim_date (full_date, year, month, day, day_of_week, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING date_id
    """, (
        date_val,
        dt.year,
        dt.month,
        dt.day,
        dt.strftime("%A"),
        dt.weekday() >= 5
    ))
    return cursor.fetchone()[0]


def load_to_warehouse(year: int, month: int, pipeline_run_id: str):
    bucket = os.getenv("S3_BUCKET_NAME")
    s3 = boto3.client("s3")
    conn = get_db_connection()
    cursor = conn.cursor()

    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    processed_key = f"processed/nyc_taxi/yellow/{year}/{month:02d}/{filename}"

    # Read validated parquet from S3
    response = s3.get_object(Bucket=bucket, Key=processed_key)
    df = pd.read_parquet(BytesIO(response['Body'].read()), engine="pyarrow")

    print(f"Loading {len(df)} rows for {year}-{month:02d}...")

    rows_loaded = 0
    df = df.head(100)  # test with 100 rows first
    for _, row in df.iterrows():
        # Resolve dimension keys
        vendor_id = get_or_create(cursor, "dim_vendor", "vendor_name",
                                   str(row["VendorID"]), "vendor_id")
        payment_type_id = get_or_create(cursor, "dim_payment_type", "payment_description",
                                         str(row["payment_type"]), "payment_type_id")
        ratecode_id = get_or_create(cursor, "dim_ratecode", "rate_description",
                                     str(row["RatecodeID"]), "ratecode_id")
        pickup_location_id = get_or_create(cursor, "dim_location", "zone_name",
                                            str(row["PULocationID"]), "location_id")
        dropoff_location_id = get_or_create(cursor, "dim_location", "zone_name",
                                             str(row["DOLocationID"]), "location_id")
        pickup_date_id = get_or_create_date(cursor, row["tpep_pickup_datetime"])
        dropoff_date_id = get_or_create_date(cursor, row["tpep_dropoff_datetime"])

        # Insert fact row
        cursor.execute("""
            INSERT INTO fact_trips (
                pickup_date_id, dropoff_date_id,
                pickup_location_id, dropoff_location_id,
                vendor_id, payment_type_id, ratecode_id,
                passenger_count, trip_distance,
                fare_amount, extra, mta_tax, tip_amount,
                tolls_amount, improvement_surcharge, total_amount,
                congestion_surcharge, airport_fee, cbd_congestion_fee,
                pickup_datetime, dropoff_datetime, pipeline_run_id
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
        """, (
            pickup_date_id, dropoff_date_id,
            pickup_location_id, dropoff_location_id,
            vendor_id, payment_type_id, ratecode_id,
            row["passenger_count"], row["trip_distance"],
            row["fare_amount"], row.get("extra"), row.get("mta_tax"),
            row["tip_amount"], row["tolls_amount"],
            row.get("improvement_surcharge"), row["total_amount"],
            row.get("congestion_surcharge"), row.get("Airport_fee"),
            row.get("cbd_congestion_fee"),
            row["tpep_pickup_datetime"], row["tpep_dropoff_datetime"],
            pipeline_run_id
        ))
        rows_loaded += 1

        # Commit every 10,000 rows to avoid huge transactions
        if rows_loaded % 10000 == 0:
            conn.commit()
            print(f"  ...{rows_loaded} rows committed")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Load complete: {rows_loaded} rows loaded for {year}-{month:02d}")
    return rows_loaded


if __name__ == "__main__":
    load_to_warehouse(2024, 3, "manual-test-run")