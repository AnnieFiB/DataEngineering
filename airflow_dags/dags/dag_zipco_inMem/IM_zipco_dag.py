from airflow.decorators import dag, task
from datetime import datetime
import os
from dotenv import load_dotenv, find_dotenv 

# Import your ETL functions here
from dag_zipco_inMem.IM_extraction import extraction
from dag_zipco_inMem.IM_transformation import transformation
from dag_zipco_inMem.IM_loading import loading

load_dotenv(find_dotenv())

@dag(
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["zipco", "etl", "in_memory"]
)
def im_real_zipco_dag():

    @task
    def extract_task():
        # extraction() should return a DataFrame (raw data)
        return extraction()

    @task
    def transform_task(raw_df):
        # transformation() should accept raw_df and return tuple of all dfs
        # (data, products, customers, staff, transactions)
        return transformation(raw_df)

    @task
    def load_task(dfs_tuple):
        # loading() should accept all dataframes as parameters
        data, products, customers, staff, transactions = dfs_tuple
        loading(data, products, customers, staff, transactions)

    raw_data = extract_task()
    transformed_data = transform_task(raw_data)
    load_task(transformed_data)

etl_dag = im_real_zipco_dag()
