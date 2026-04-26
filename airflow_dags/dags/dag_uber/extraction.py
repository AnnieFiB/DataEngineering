# ==================================================================================
# # Batch ETL Pipeline (Cloud deployment)
# ## Case Study - Cloud -Azure Blob Storage, Azure Databricks, synapse, data factory
# ==================================================================================
# ### Libraries and Dependencies

import pandas as pd
import gdown
import tempfile
import shutil
import os
import sys
import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())  

DRIVE_FILE_ID="1GRyRW2pW3isbbwBPW7lcJrS-yeuZGPj_"
gdrive_url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}"
connect_str = os.getenv('UBER_STORAGE_CONNECTION_STRING')
container_name = os.getenv('uber_container_name')
BLOB_PATH  = os.getenv("BLOB_PATH", "raw-data/ncr_ride_bookings.parquet")

# ====================
# ### Ingestion: Extraction Layer
# ====================

def upload_dataframe_to_blob(df, container_name, blob_name):
    # Convert DataFrame to Parquet format
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)  

     # Upload to Azure Blob Storage
    print("Connecting to azure Blob Storage...")
    # Set up a connection to azure blob storage

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    print('connection succesful...')
   
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, overwrite=True)
    print(f"'{df}' uploaded to Azure Blob Storage in container '{container_name}/{blob_name}' with {df.shape[0]} rows.")   

    

def extract_and_upload():
    """
    Download from Google Drive → upload to Azure Blob Storage.
    Returns: (container_name, blob_path)
    """
    print("Extracting uber rides raw data (csv) from google drive")
    # - Download CSV content from Google Drive into a temporary file path 
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp_path = tmp.name

        gdown.download(f"https://drive.google.com/uc?id={DRIVE_FILE_ID}", tmp_path, quiet=False)
        raw_data = pd.read_csv(tmp_path)
    
    # Upload to Azure Blob Storage

    try:
        upload_dataframe_to_blob(raw_data, container_name, blob_name=BLOB_PATH)
        return container_name, BLOB_PATH 
            
    except (ImportError, Exception) as e:
            print(f"Azure upload failed: {str(e)}")
            print("Falling back to local storage...")
            
            local_path = "ncr_ride_bookings.csv"
            shutil.copy2(tmp_path, local_path)
            print(f"File saved locally as: {local_path}")
            return "local", local_path
            
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

