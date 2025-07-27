import os
import pandas as pd

def extraction():
    try:
        base_path = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_path, "zipco_transaction.csv")
        data = pd.read_csv(file_path)
        print("Data extracted successfully.")
        return data
    except Exception as e:
        print(f"Extraction failed: {e}")
        raise
