from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from utils import load_environment_variables

# Import the tasks from their respective modules
from tmdb_extract_movie import extract_movie_data
from tmdb_transform_movie import transform_movies
from tmdb_load_movie import load_to_db


load_environment_variables()

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 7, 26),
}

dag = DAG(
    'tmdb_movie_data_etl',
    default_args=default_args,
    description='ETL process for movie data from TMDB API to PostgreSQL DB and Azure Blob Storage',
    schedule_interval=None,  
)

def extract_task_func(**kwargs):
    start, end = kwargs['start'], kwargs['end']
    extracted_data, combined_data = extract_movie_data(start, end)
    kwargs['ti'].xcom_push(key='extracted_data', value=combined_data)

def transform_task_func(**kwargs):
    combined_data = kwargs['ti'].xcom_pull(task_ids='extract_task', key='extracted_data')
    if combined_data is not None:
        transformed_data = transform_movies(combined_data)
        kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)
    else:
        raise ValueError("No data was extracted, transformation cannot proceed.")

def upload_to_azure(df, file_name: str):
    """
    Upload the cleaned CSV to Azure Blob Storage.
    """
    # Retrieve Azure connection string and container name from environment variables
    connect_str = os.getenv("TMDB_AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("TMDB_container_name")

    if connect_str is None or container_name is None:
        raise ValueError("Azure connection string or container name is not set in environment variables.")

    # Create BlobServiceClient and ContainerClient
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    # Convert DataFrame to CSV and upload to Azure
    file_path = os.path.join(os.getenv("AIRFLOW_HOME"), 'dags', file_name)
    df.to_csv(file_path, index=False)

    blob_client = container_client.get_blob_client(file_name)

    # Upload the CSV file
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded {file_name} to Azure Blob Storage.")

def load_task_func(**kwargs):
    # Get the transformed data from XCom
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_task', key='transformed_data')
    if transformed_data is not None:
        db_url = os.getenv("BASE_URL")  # Assuming the DB URL is set in your .env file
        load_to_db(transformed_data, db_url, table_name="movies")

        # After loading to DB, upload the cleaned data to Azure
        upload_to_azure(transformed_data, "CleanedMovies.csv")
    else:
        raise ValueError("No data to load, transformation failed.")

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task_func,
    op_kwargs={'start': 1, 'end': 10},  # Example page range for extracting data
    provide_context=True,  # Allow access to XCom
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_task_func,
    provide_context=True,  # Allow access to XCom
    dag=dag
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_task_func,
    provide_context=True,  # Allow access to XCom
    dag=dag
)

# Set up the task dependencies
extract_task >> transform_task >> load_task
