import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv, find_dotenv
import os


def loading():
    base_path = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(find_dotenv())

    
    # Read local files
    data = pd.read_csv(os.path.join(base_path, 'clean_data.csv'))
    products = pd.read_csv(os.path.join(base_path, 'products.csv'))
    customers = pd.read_csv(os.path.join(base_path, 'customers.csv'))
    staff = pd.read_csv(os.path.join(base_path, 'staff.csv'))
    transactions = pd.read_csv(os.path.join(base_path, 'transactions.csv'))

    # Connect to Azure Blob
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    container_name = os.getenv('zipco_container_name')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    files = [
        (data, "rawdata/cleaned_zipco_transaction_data.csv"),
        (products, "cleaneddata/products.csv"),
        (customers, "cleaneddata/customers.csv"),
        (staff, "cleaneddata/staff.csv"),
        (transactions, "cleaneddata/transactions.csv")
    ]

    # Upload each file
    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f"{blob_name} loaded into Azure Blob Storage.")
