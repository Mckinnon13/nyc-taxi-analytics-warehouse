# dags/nyc_taxi_pipeline_dag.py

from airflow.decorators import dag, task
from datetime import datetime
import sys
import os

sys.path.append('/opt/airflow/src')

from extract.download_taxi_data import download_to_s3
from validate.validate_taxi_data import validate_taxi_data


@dag(
    dag_id="nyc_taxi_monthly_pipeline",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    default_args={"retries": 0},
    tags=["nyc-taxi", "project-1"],
)
def nyc_taxi_pipeline():

    @task
    def extract_task(**context):
        execution_date = context["logical_date"]
        year = execution_date.year
        month = execution_date.month
        downloaded = download_to_s3(year, month)
        return {"year": year, "month": month, "downloaded": downloaded}

    @task
    def validate_task(extract_result: dict):
        if not extract_result["downloaded"]:
            print(
                f"Skipping validation — {extract_result['year']}-"
                f"{extract_result['month']:02d} was not downloaded."
            )
            return
        validate_taxi_data(extract_result["year"], extract_result["month"])

    result = extract_task()
    validate_task(result)


nyc_taxi_pipeline()