import os
from dotenv import load_dotenv


# Function to dynamically get file paths
def get_file_path(file_name):
    base_path = os.path.join(os.getenv("AIRFLOW_HOME", "."), "dags")
    return os.path.join(base_path, file_name)

# Function to load environment variables
def load_environment_variables():
    load_dotenv(dotenv_path=get_file_path('.env'))

