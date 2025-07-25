from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from extraction import run_extraction
from transformation import run_transformation
from loading import run_loading


# Airflow DAG for Zipco Foods ETL Pipeline
# This DAG orchestrates the extraction, transformation, and loading of data for Zipco Foods.

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 22),
    'email': 'gwitanniemona@outlook.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retries_delay': timedelta(minutes=1)
}

dag = DAG(
    "zipco_foods_pipeline",
    default_args=default_args,
    schedule=timedelta(days=1),
    catchup=False
)

extraction = PythonOperator(
    task_id='extraction_layer',
    python_callable=run_extraction,
    dag=dag,
)

transformation = PythonOperator(
    task_id='transformation_layer',
    python_callable=run_transformation,
    dag=dag,
)

loading = PythonOperator(
    task_id='loading_layer',
    python_callable=run_loading,
    dag=dag,
)

extraction >> transformation >> loading