import pandas as pd
import os

def extraction():
    try:
        file_path = os.path.join(os.getenv("AIRFLOW_HOME", "."), "dags", "zipco_transaction.csv")
        data = pd.read_csv(file_path)
        print("Data loaded successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")
