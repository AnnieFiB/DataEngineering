from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from extract import extract_movies
from transform import transform_movies
from load import load_movies

default_args = {
    'owner': 'airflow',                     # Who is responsible for the DAG
    'depends_on_past': False,               # Don't wait on previous runs
    'start_date': datetime(2025, 7, 19),  # When the DAG should start running
    'email': ['gwitanniemona@outlook.com'],    # Alert emails
    'email_on_failure': True,              # Set to True if you want email alerts
    'email_on_retry': True,
    'retries': 2,                           # Number of retries
    'retry_delay': timedelta(minutes=5),    # Wait between retries
    'catchup': False                         # Skip backfilling
}

with DAG(
    'movie_recommendation_system_dag',
    default_args=default_args,
    description='A Simple pipeline used to extract, transform, and load movie data from TMDB API to a postgres database',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False
) as dag:

    # Task to extract movie data
    extract_task = PythonOperator(
        task_id='extract_movies',
        python_callable=extract_movies,
        provide_context=True
    )

    # Task to transform movie data
    transform_task = PythonOperator(
        task_id='transform_movies',
        python_callable=transform_movies,
        provide_context=True        
    )

    # Task to load movie data into the database
    load_task = PythonOperator(
        task_id='load_movies',
        python_callable=load_movies,
        provide_context=True
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task