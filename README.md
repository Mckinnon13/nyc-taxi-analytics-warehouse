# NYC Taxi Analytics Warehouse

A production-style batch data pipeline that ingests NYC TLC Yellow Taxi trip data monthly, validates it, and loads it into a star schema data warehouse — orchestrated with Apache Airflow.

---

## Architecture

```
NYC TLC Source (CloudFront CDN)
        │
        ▼
  [Extract Task]
  Stream Parquet → S3 raw/
        │
        ▼
  [Validate Task]
  Apply business rules
  Good rows → S3 processed/
  Bad rows  → S3 rejected/
        │
        ▼
  [Load Task]
  Read processed/ from S3
  Resolve dimension keys
  Insert into PostgreSQL star schema
        │
        ▼
  PostgreSQL Warehouse
  (fact_trips + 5 dimension tables)
```

**Tech stack**: Python · Apache Airflow · PostgreSQL · AWS S3 · boto3 · pandas · pyarrow · Docker Compose

---

## Project Structure

```
nyc-taxi-analytics-warehouse/
├── dags/
│   └── nyc_taxi_pipeline_dag.py     # Airflow DAG (extract → validate → load)
├── src/
│   ├── extract/
│   │   └── download_taxi_data.py    # Stream TLC Parquet to S3 raw zone
│   ├── validate/
│   │   └── validate_taxi_data.py    # Business rule validation, good/bad split
│   └── load/
│       └── load_to_warehouse.py     # Star schema load into PostgreSQL
├── sql/
│   ├── create_star_schema.sql       # DDL for all dim/fact tables
│   └── create_pipeline_run_log.sql  # DDL for pipeline audit table
├── docker/
│   ├── docker-compose.yml           # PostgreSQL (warehouse) + PostgreSQL (Airflow) + Airflow
│   ├── Dockerfile                   # Custom Airflow image with pipeline dependencies
│   ├── airflow_requirements.txt     # Python packages installed in Airflow container
│   └── .env.example                 # Environment variable template (copy to .env)
├── docs/
│   └── LEARNING_NOTES.md            # Running notes on tools and concepts
├── tests/                           # Unit and integration tests (to be added)
├── requirements.txt                 # Python packages for local development
└── README.md
```

---

## Data Model (Star Schema)

```
                    dim_date
                       │
        dim_vendor ─── fact_trips ─── dim_location (pickup)
                    │              └── dim_location (dropoff)
        dim_payment_type
                    │
               dim_ratecode
```

### Fact Table: `fact_trips`
One row per validated taxi trip. Contains all measurable columns (fares, distances, counts) plus foreign keys to all dimension tables and a `pipeline_run_id` for lineage tracing.

### Dimension Tables
| Table | Description | Rows (approx) |
|---|---|---|
| `dim_date` | One row per calendar date (year, month, day, weekday, is_weekend) | Dynamic |
| `dim_location` | NYC TLC taxi zones (~265 zones) with borough and service zone | ~265 |
| `dim_vendor` | Taxi vendor/company lookup | 2-3 |
| `dim_payment_type` | Payment method lookup (cash, credit card, etc.) | ~6 |
| `dim_ratecode` | Rate code lookup (standard, JFK, Newark, etc.) | ~7 |

### Audit Table: `pipeline_run_log`
Tracks every pipeline run with status (`IN_PROGRESS` → `SUCCESS` / `SKIPPED_NOT_PUBLISHED`), row counts, and timestamps.

---

## Pipeline Design Decisions

**Batch, not streaming** — TLC data is published monthly with a ~2-month lag. Real-time processing would add infrastructure complexity with zero business value for this source.

**2-month offset** — The DAG uses `logical_date - 2 months` as the target month, accounting for TLC's real publishing lag. Runs for unpublished months gracefully skip with `SKIPPED_NOT_PUBLISHED` status rather than failing.

**Idempotency** — Both extract and validate steps are safe to re-run: extract checks S3 key existence before downloading; load uses `get_or_create` for dimension rows to avoid duplicates.

**Dead-letter pattern** — Invalid rows (negative fares, zero passengers, pickup after dropoff) are not silently dropped — they go to `s3://de-nyc-taxi/rejected/` for inspection and potential reprocessing.

**Separation of concerns** — DAG files contain only task wiring; all business logic lives in `src/` to avoid Airflow's ~30-second scheduler re-parse overhead hitting heavy code.

---

## Getting Started

### Prerequisites
- Docker Desktop
- AWS account with S3 access
- Python 3.9+ (for local script execution)

### 1. Clone the repo
```bash
git clone https://github.com/<your-username>/nyc-taxi-analytics-warehouse.git
cd nyc-taxi-analytics-warehouse
```

### 2. Configure environment variables
```bash
cp docker/.env.example docker/.env
# Edit docker/.env with your actual values:
# - AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
# - S3 bucket name
# - PostgreSQL credentials
```

### 3. Start the stack
```bash
cd docker
docker-compose up -d --build
```

Wait ~60 seconds for Airflow to initialize, then open `http://localhost:8080`.
Default credentials: `admin` / (check `docker logs docker-airflow-1` for generated password)

### 4. Initialize the warehouse schema
```bash
docker cp ../sql/create_star_schema.sql docker-postgres-warehouse-1:/create_star_schema.sql
docker cp ../sql/create_pipeline_run_log.sql docker-postgres-warehouse-1:/create_pipeline_run_log.sql

docker exec -it docker-postgres-warehouse-1 psql -U warehouse_user -d nyc_taxi_warehouse \
  -f /create_star_schema.sql \
  -f /create_pipeline_run_log.sql
```

### 5. Enable and trigger the DAG
In the Airflow UI (`http://localhost:8080`):
1. Toggle `nyc_taxi_monthly_pipeline` from Paused → Active
2. Click ▶ to trigger a manual run, or let the monthly schedule run automatically

The DAG will backfill all months from `2024-01-01` to now (minus 2 months for TLC publishing lag).

---

## S3 Bucket Structure

```
de-nyc-taxi/
├── raw/nyc_taxi/yellow/{year}/{month}/          # Original TLC Parquet files
├── processed/nyc_taxi/yellow/{year}/{month}/    # Validated good rows
├── rejected/nyc_taxi/yellow/{year}/{month}/     # Rows that failed validation
├── audit/                                        # Pipeline run metadata snapshots
└── archive/                                      # Post-processing file archive
```

---

## Validation Rules

Rows are flagged as invalid if any of the following are true:
- `fare_amount < 0` (negative fare)
- `passenger_count == 0` (no passengers)
- `tpep_pickup_datetime > tpep_dropoff_datetime` (pickup after dropoff)

Invalid rows go to `rejected/` in S3. Valid rows go to `processed/` and are loaded into `fact_trips`.

---

## Known Gaps / Future Improvements

- **`dim_location` missing real zone names** — currently stores raw zone IDs. Fix: load TLC's `taxi_zone_lookup.csv` to populate borough and zone name properly.
- **Row-by-row insert in load script** — `load_to_warehouse.py` uses `df.iterrows()` which is slow for 3.5M rows. Fix: migrate to `psycopg2.extras.execute_batch()` or PostgreSQL `COPY` for bulk loading.
- **SMTP not configured** — email alerting flags are set in the DAG (`email_on_failure=True`) but SMTP credentials are not configured. Fix: add `[smtp]` section to `airflow.cfg` with Gmail app password or relay credentials.
- **No unit tests** — `tests/` directory is scaffolded but empty. Fix: add unit tests for validation logic (mask correctness) and load logic (dimension key resolution).
- **`store_and_fwd_flag` not loaded** — this column from the source is currently dropped. Could be added to `fact_trips` as a boolean attribute if needed.

---

## Data Source

NYC TLC Yellow Taxi Trip Records — publicly available at:
`https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`

Raw files downloaded from CloudFront CDN:
`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YYYY}-{MM}.parquet`

---

## License

MIT