import pandas as pd

def extraction():
    try:
        data = pd.read_csv(r'zipco_transaction.csv')
        print("Data loaded successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")