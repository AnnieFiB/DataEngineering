import os
import pandas as pd

def extraction():
    try:
        file_path = os.path.join(os.getenv("AIRFLOW_HOME", "."), "dags", "zipco_transaction.csv")
        data = pd.read_csv(file_path)
        print("Data extracted successfully.")
        return data
    except Exception as e:
        print(f"Extraction failed: {e}")
        raise
