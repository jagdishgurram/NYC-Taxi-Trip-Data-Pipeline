from airflow.sdk import dag, task
from datetime import datetime,timedelta

from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from testing.test_datasets import run_all_validations
from etl.star import create_star_schema
from airflow.models import Variable

email = Variable.get("alert_email", default_var=None)

default_args = {
    "owner": "Jagdish",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": [email],
    "email_on_failure": True,
    "email_on_retry": True,
    "email_on_success": False,   # Optional
    }

@dag(
    dag_id="nyc_taxi_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
)

def pipeline_nyc():
    @task.python
    def extract_task():
        extract_data()
        return "Data Extraction Completed"

    @task.python
    def transform_task():
        transform_data()
        return "Data Transformation Completed"
        

    @task.python
    def validate_task():
        run_all_validations()
        return "Data Validation Completed"

    @task.python
    def load_task():
        load_data()
        return "Data Loading Completed"

    @task.python
    def star_task():
        create_star_schema()
        return "Star Schema Created"

    # Defining Dag Dependencies
    extract = extract_task()
    transform = transform_task(extract)
    validate = validate_task(transform)
    load = load_task(validate)
    star = star_task(load)

    extract >> transform >> validate >> load >> star

pipeline_nyc()
