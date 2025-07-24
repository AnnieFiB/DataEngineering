
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
from io import StringIO, BytesIO
import os

# Load environment variables from .env file
load_dotenv()

def run_loading(dfs):

    print("/n Starting data load to Azure Blob Storage...")

    # Create Azure connection string and container name and a BlobServiceClient object
    connect_str = os.getenv('ZIPCO_AZURE_STORAGE_CONNECTION_STRING')
    container_name = os.getenv('zipco_container_name')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    # container_client = blob_service_client.get_container_client(container_name)

    # Load data to Azure Blob Storage
   
    # Upload Multiple DataFrames as CSVs In-Memory
    for name, df in dfs.items():
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{name}.csv")
        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        print(f" Uploaded {name}.csv into Azure Blob Storage")

    print("Data load to Azure Blob Storage completed successfully.")

# Alternative: Upload Multiple DataFrames as Parquet Files In-Memory
# for name, df in dfs.items():
#     buffer = BytesIO()
#     df.to_parquet(buffer, index=False)
#     buffer.seek(0)
#     blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{name}.parquet")
#     blob_client.upload_blob(buffer, overwrite=True)
#     print(f" Uploaded {name}.parquet into Azure Blob Storage")
    
# Alternative: Upload Multiple DataFrames as JSON Files In-Memory
# for name, df in dfs.items():   
#     json_buffer = StringIO()
#     df.to_json(json_buffer, orient='records', lines=True)
#     blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{name}.json")
#     blob_client.upload_blob(json_buffer.getvalue(), overwrite=True)
#     print(f" Uploaded {name}.json into Azure Blob Storage")
    
# Alternative: --Save normalized tables to new CSV files
# data.to_csv('data/clean_data.csv', index=False)
# products.to_csv('data/products.csv', index=False)
# customers.to_csv('data/customers.csv', index=False)
# staff.to_csv('data/staff.csv', index=False)
# transactions.to_csv('data/transactions.csv', index=False)
        
    