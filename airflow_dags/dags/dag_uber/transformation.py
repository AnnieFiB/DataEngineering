# ==================================================================================
# # Batch ETL Pipeline (Cloud deployment)
# ## Case Study - Azure Blob Storage, Azure Databricks, synapse, data factory
# ==================================================================================
# ### Libraries and Dependencies

import pandas as pd
import numpy as np
from datetime import datetime
import re
import os
import io
import sys
from azure.storage.blob import BlobServiceClient
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

out_prefix = os.getenv("OUT_PREFIX", "transformed-data")
BLOB_PATH = os.getenv("BLOB_PATH", "raw-data/ncr_ride_bookings.parquet")
connect_str = os.getenv('UBER_STORAGE_CONNECTION_STRING')
container_name = os.getenv('uber_container_name')



# ====================
# ### Data Cleaning Functions
# ====================
def standardize_columns(df):
    """Standardize column names to snake_case."""
    new_columns = {}
    for col in df.columns:
        new_col = (
            col.strip()
            .lower()
            .replace(" ", "_")
        )
        new_col = re.sub(r"[^\w_]", "", new_col)
        new_columns[col] = new_col
    
    df = df.rename(columns=new_columns)
    return df


def parse_datetime(df):
    """Parse date + time into timestamp, date_id."""
    try:
        if "date" in df.columns and "time" in df.columns:
            df["booking_timestamp"] = pd.to_datetime(
                df["date"].astype(str) + " " + df["time"].astype(str), 
                format="%Y-%m-%d %H:%M:%S",
                errors='coerce'
            )
        elif "date" in df.columns:
            df["booking_timestamp"] = pd.to_datetime(df["date"], errors='coerce')
        else:
            df["booking_timestamp"] = None
        
        # Create date_id from timestamp
        df["date_id"] = df["booking_timestamp"].dt.strftime("%Y%m%d").astype(int)
        
    except Exception as e:
        print(f"Error parsing datetime: {e}")
        df["booking_timestamp"] = None
        df["date_id"] = None
    
    return df


def normalize_strings(df):
    """Normalize categorical columns."""
    if "booking_status" in df.columns:
        df["booking_status"] = df["booking_status"].astype(str).str.title().str.strip()

    if "payment_method" in df.columns:
        pm_map = {
            "Upi": "UPI", 
            "Debit Card": "Debit Card",
            "Credit Card": "Credit Card", 
            "Cash": "Cash"
        }
        df["payment_method"] = df["payment_method"].astype(str).str.strip().map(pm_map).fillna(df["payment_method"])
    
    return df


def clean_numeric_columns(df):
    """Clean and convert numeric columns."""
    numeric_columns = ["avg_vtat", "avg_ctat", "booking_value", "ride_distance", "driver_ratings", "customer_rating"]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def handle_missing_values(df):
    """Handle missing values in the dataset."""
    # Fill missing categorical values
    categorical_cols = ["booking_status", "payment_method", "vehicle_type"]
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")
    
    # Fill missing numeric values with 0 or mean
    numeric_cols = ["avg_vtat", "avg_ctat", "booking_value", "ride_distance", "driver_ratings", "customer_rating"]
    for col in numeric_cols:
        if col in df.columns:
            if df[col].dtype in ['int64', 'float64']:
                df[col] = df[col].fillna(0)
    
    return df


# ====================
# ### Dimension Building Functions
# ====================
def build_dim_date(df):
    
    if "booking_timestamp" not in df.columns:
        return pd.DataFrame()
    
    date_dim = df[["date_id", "booking_timestamp"]].dropna().drop_duplicates()
    date_dim["date"] = date_dim["booking_timestamp"].dt.date
    date_dim["year"] = date_dim["booking_timestamp"].dt.year
    date_dim["month"] = date_dim["booking_timestamp"].dt.month
    date_dim["quarter"] = date_dim["booking_timestamp"].dt.quarter
    date_dim["day_of_week"] = date_dim["booking_timestamp"].dt.day_name()
    
    return date_dim[["date_id", "date", "year", "month", "quarter", "day_of_week"]].reset_index(drop=True)


def build_dim_customer(df):

    if "customer_id" not in df.columns:
        return pd.DataFrame()
    
    customer_dim = df[["customer_id"]].dropna().drop_duplicates()
    customer_dim["customer_sk"] = range(1, len(customer_dim) + 1)
    
    return customer_dim[["customer_sk", "customer_id"]].reset_index(drop=True)


def build_dim_vehicle(df):
  
    if "vehicle_type" not in df.columns:
        return pd.DataFrame()
    
    vehicle_dim = df[["vehicle_type"]].dropna().drop_duplicates()
    vehicle_dim["vehicle_sk"] = range(1, len(vehicle_dim) + 1)
    
    return vehicle_dim[["vehicle_sk", "vehicle_type"]].reset_index(drop=True)


def build_dim_location(df):
   
    location_cols = ["pickup_location", "drop_location"]
    available_cols = [col for col in location_cols if col in df.columns]
    
    if not available_cols:
        return pd.DataFrame()
    
    location_dim = df[available_cols].dropna().drop_duplicates()
    location_dim = location_dim.melt(value_name="location_name").dropna()
    location_dim = location_dim[["location_name"]].drop_duplicates()
    location_dim["location_sk"] = range(1, len(location_dim) + 1)
    
    return location_dim[["location_sk", "location_name"]].reset_index(drop=True)


def build_dim_payment(df):
   
    if "payment_method" not in df.columns:
        return pd.DataFrame()
    
    payment_dim = df[["payment_method"]].dropna().drop_duplicates()
    payment_dim["payment_sk"] = range(1, len(payment_dim) + 1)
    
    return payment_dim[["payment_sk", "payment_method"]].reset_index(drop=True)


def build_dim_reason(df):

    reason_cols = ["reason_for_cancelling_by_customer", "driver_cancellation_reason", "incomplete_rides_reason"]
    available_cols = [col for col in reason_cols if col in df.columns]
    
    reason_data = []
    for col in available_cols:
        values = df[col].dropna().unique()
        reason_data.extend(values)

    unique_reasons = list(set(reason_data))
    reason_dim = pd.DataFrame({
        "reason_text": unique_reasons,
        "reason_sk": range(1, len(unique_reasons) + 1)
    })
    return reason_dim[["reason_sk", "reason_text"]].reset_index(drop=True)

# ====================
# ### Fact Table Building
# ====================
def build_fact_booking(df, dim_customer, dim_vehicle, dim_location, dim_payment, dim_reason):

    fact_df = df.copy()
    
    if not dim_customer.empty:
        fact_df = fact_df.merge(dim_customer, on="customer_id", how="left")
    
    if not dim_vehicle.empty:
        fact_df = fact_df.merge(dim_vehicle, on="vehicle_type", how="left")
    
    if not dim_location.empty:
        fact_df = fact_df.merge(dim_location, left_on="pickup_location", right_on="location_name", how="left")
        fact_df = fact_df.rename(columns={"location_sk": "pickup_location_sk"})
        fact_df = fact_df.merge(dim_location, left_on="drop_location", right_on="location_name", how="left")
        fact_df = fact_df.rename(columns={"location_sk": "drop_location_sk"})
    
    if not dim_payment.empty:
        fact_df = fact_df.merge(dim_payment, on="payment_method", how="left")
        fact_df = fact_df.rename(columns={"payment_sk": "payment_sk"})
    
    if not dim_reason.empty:
        fact_df = fact_df.merge(dim_reason, left_on="reason_for_cancelling_by_customer", right_on="reason_text", how="left")
        fact_df = fact_df.rename(columns={"reason_sk": "customer_cancel_reason_sk"})
        fact_df = fact_df.merge(dim_reason, left_on="driver_cancellation_reason", right_on="reason_text", how="left")
        fact_df = fact_df.rename(columns={"reason_sk": "driver_cancel_reason_sk"})
        fact_df = fact_df.merge(dim_reason, left_on="incomplete_rides_reason", right_on="reason_text", how="left")
        fact_df = fact_df.rename(columns={"reason_sk": "incomplete_reason_sk"})
    
    fact_columns = [
        "booking_id", "date_id", "customer_sk", "vehicle_sk", 
        "pickup_location_sk", "drop_location_sk", "payment_sk",
        "customer_cancel_reason_sk", "driver_cancel_reason_sk", "incomplete_reason_sk",
        "booking_status", "avg_vtat", "avg_ctat", "booking_value",
        "ride_distance", "driver_ratings", "customer_rating"
    ]
    
    available_columns = [col for col in fact_columns if col in fact_df.columns]
    return fact_df[available_columns].reset_index(drop=True)


def get_blob_object(container_name, blob_path):
    """Get Azure blob object from container and path."""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_path)
        return blob_client
    
    except Exception as e:
        print(f"Failed to get blob object: {str(e)}")
        return None

def read_blob_data(blob_object):
    """Read data from Azure blob object."""
    try:
        # Download blob content and convert to bytes
        blob_data = blob_object.download_blob()
        content = blob_data.readall()  
        
        print("Reading data from Azure blob...")
        
        if blob_object.blob_name.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(content))
            return df
        
        elif blob_object.blob_name.endswith('.parquet'):
            df = pd.read_parquet(io.BytesIO(content))
            return df
            
    except Exception as e:
        print(f"Failed to read blob data: {str(e)}")
        return None

# ====================
# ### Main Transformation Function
# ====================

def transform_data(blob_path):

    print("Starting data transformation...")   
    print("Reading data from Azure blob...")

    blob_object = get_blob_object(container_name, blob_path)
    df = read_blob_data(blob_object)
    
    if df is None or df.empty:
        print("No data to transform!")
        return {}
    
    print(f"Loaded {len(df)} rows from blob")
    
    # Step A: Clean and standardize
    print("Step 1: Standardizing columns...")
    cleaned_df = standardize_columns(df)
    
    print("Step 2: Parsing datetime...")
    cleaned_df = parse_datetime(cleaned_df)
    
    print("Step 3: Normalizing strings...")
    cleaned_df = normalize_strings(cleaned_df)
    
    print("Step 4: Cleaning numeric columns...")
    cleaned_df = clean_numeric_columns(cleaned_df)
    
    print("Step 5: Handling missing values...")
    cleaned_df = handle_missing_values(cleaned_df)

    # Step B: Normalise and Build Dim & Fact
    #     
    print("Step 6: Building dimension tables...")
    dim_date = build_dim_date(cleaned_df)
    dim_customer = build_dim_customer(cleaned_df)
    dim_vehicle = build_dim_vehicle(cleaned_df)
    dim_location = build_dim_location(cleaned_df)
    dim_payment = build_dim_payment(cleaned_df)
    dim_reason = build_dim_reason(cleaned_df)
    
    print("Step 7: Building fact table...")
    fact_booking = build_fact_booking(cleaned_df, dim_customer, dim_vehicle, dim_location, dim_payment, dim_reason)
    
    print("Data transformation completed successfully!")
    
    return {
        "cleaned_data": cleaned_df,
        "dim_date": dim_date,
        "dim_customer": dim_customer,
        "dim_vehicle": dim_vehicle,
        "dim_location": dim_location,
        "dim_payment": dim_payment,
        "dim_reason": dim_reason,
        "fact_booking": fact_booking
        }

