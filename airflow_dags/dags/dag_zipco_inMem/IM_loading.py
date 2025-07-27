import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv, find_dotenv
import os
from io import StringIO



def loading(data, products, customers, staff, transactions):
    base_path = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(find_dotenv())

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

    for df, blob_name in files:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        print(f"Uploaded: {blob_name}")
