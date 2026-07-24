# dags/nyc_taxi_pipeline_dag.py

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
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
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email": ["your_email@example.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        # NOTE: SMTP not configured locally — email delivery requires
        # Airflow SMTP config (see airflow.cfg [smtp] section). Flags left
        # in place to reflect intended alerting behavior.
    },
    tags=["nyc-taxi", "project-1"],
)
def nyc_taxi_pipeline():

    @task
    def extract_task(**context):
        execution_date = context["logical_date"]
        target_date = execution_date - relativedelta(months=2)
        year = target_date.year
        month = target_date.month
        file_ready = download_to_s3(year, month)
        return {"year": year, "month": month, "file_ready": file_ready}

    @task
    def validate_task(extract_result: dict):
        if not extract_result["file_ready"]:
            print(
                f"Skipping validation — {extract_result['year']}-"
                f"{extract_result['month']:02d} was not downloaded."
            )
            return
        validate_taxi_data(extract_result["year"], extract_result["month"])

    result = extract_task()
    validate_task(result)


nyc_taxi_pipeline()