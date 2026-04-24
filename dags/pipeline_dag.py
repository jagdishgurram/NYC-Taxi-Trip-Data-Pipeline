from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta

from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from validation.validate import run_all_validations
from etl.star import create_star_schema

default_args = {
    "owner": "jagdish",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}   

with DAG(
    dag_id="nyc_taxi_pipeline",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:
    
    extract_task =  PythonOperator(
        task_id = 'extract_data',
        python_callable=extract_data,
        dag=dag
    )
    
    transform_task =  PythonOperator(
        task_id = 'transform_data',
        python_callable=transform_data,
        dag=dag
    )
    
    validate_task =  PythonOperator(
        task_id = 'validate_data',
        python_callable=run_all_validations,
        dag=dag
    )
    
    load_task =  PythonOperator(
        task_id = 'load_data',
        python_callable=load_data,
        dag=dag
    ) 
    
    star_task =  PythonOperator(
        task_id = 'create_star_schema',
        python_callable=create_star_schema,
        dag=dag
    )     
    
    extract_task >> transform_task >> validate_task >> load_task >> star_task