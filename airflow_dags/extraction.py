import pandas as pd

def run_extraction():
    try:
        df = pd.read_csv(r'zipco_transaction.csv')
        print("Data loaded successfully!")
        print(f"Current shape: {df.shape}")
    except Exception as e:
        print(f"An error occurred: {e}")
        df = pd.DataFrame()  # Return an empty DataFrame on error

    return df

