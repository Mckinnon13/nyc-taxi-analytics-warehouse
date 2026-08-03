-- sql/create_star_schema.sql

CREATE TABLE dim_date (
    date_id      SERIAL PRIMARY KEY,
    full_date    DATE NOT NULL,
    year         INT NOT NULL,
    month        INT NOT NULL,
    day          INT NOT NULL,
    day_of_week  VARCHAR(10) NOT NULL,
    is_weekend   BOOLEAN NOT NULL
);

CREATE TABLE dim_location (
    location_id  SERIAL PRIMARY KEY,
    borough      VARCHAR(50),
    zone_name    VARCHAR(100) NOT NULL,
    service_zone VARCHAR(50)
);

CREATE TABLE dim_vendor (
    vendor_id    SERIAL PRIMARY KEY,
    vendor_name  VARCHAR(100) NOT NULL
);

CREATE TABLE dim_ratecode (
    ratecode_id      SERIAL PRIMARY KEY,
    rate_description VARCHAR(100) NOT NULL
);

CREATE TABLE dim_payment_type (
    payment_type_id     SERIAL PRIMARY KEY,
    payment_description VARCHAR(100) NOT NULL
);

CREATE TABLE fact_trips (
    trip_id             SERIAL PRIMARY KEY,
    pickup_date_id      INT NOT NULL REFERENCES dim_date(date_id),
    dropoff_date_id     INT NOT NULL REFERENCES dim_date(date_id),
    pickup_location_id  INT NOT NULL REFERENCES dim_location(location_id),
    dropoff_location_id INT NOT NULL REFERENCES dim_location(location_id),
    vendor_id           INT NOT NULL REFERENCES dim_vendor(vendor_id),
    payment_type_id     INT NOT NULL REFERENCES dim_payment_type(payment_type_id),
    ratecode_id         INT NOT NULL REFERENCES dim_ratecode(ratecode_id),
    passenger_count     INT NOT NULL,
    trip_distance       DECIMAL(10,2) NOT NULL,
    fare_amount         DECIMAL(10,2) NOT NULL,
    extra               DECIMAL(10,2),
    mta_tax             DECIMAL(10,2),
    tip_amount          DECIMAL(10,2) NOT NULL,
    tolls_amount        DECIMAL(10,2) NOT NULL,
    improvement_surcharge DECIMAL(10,2),
    total_amount        DECIMAL(10,2) NOT NULL,
    congestion_surcharge DECIMAL(10,2),
    airport_fee         DECIMAL(10,2),
    cbd_congestion_fee  DECIMAL(10,2),
    pickup_datetime     TIMESTAMP NOT NULL,
    dropoff_datetime    TIMESTAMP NOT NULL,
    pipeline_run_id     VARCHAR(50) NOT NULL
);