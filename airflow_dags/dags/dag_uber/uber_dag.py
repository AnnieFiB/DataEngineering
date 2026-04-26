from airflow.decorators import dag, task
from datetime import datetime
import os
import sys
from dotenv import load_dotenv, find_dotenv 

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import your ETL functions here
import extraction
import transformation
import loading

load_dotenv(find_dotenv())

@dag(
    schedule_interval=None,
    start_date=datetime(2025, 8, 22),
    catchup=False,
    description='ETL process for uber ride data from google drive to Azure Blob Storage',
    tags=["uber", "etl", "dag"]
)
def uber_bookings_dag():

    @task
    def extract_task():
         _, blob_path =  extraction.extract_and_upload()
         return blob_path

    @task
    def transform_task(blob_path):
        # transformation() should return dict of all dfs (cleaned_data,date, dim, facts)
        return transformation.transform_data(blob_path)

    @task
    def load_task(transformed_data):
                   
            out_prefix = os.getenv('output_prefix', 'transformed-data')
            container_name = os.getenv('uber_container_name')
            return  loading.load_transformed_data_to_gcs_db(transformed_data, container_name, out_prefix, db_config=None)

    
    # Execute the pipeline    
    blob_path =  extract_task()
    transformed_dict = transform_task(blob_path)
    load_task(transformed_dict)

etl_dag = uber_bookings_dag()