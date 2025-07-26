#!/usr/bin/env python
# coding: utf-8

# # Airflow Note

# In[8]:


display(Image("algorithm.png"))
display(Image("airflowsession.png")) 


# # Case Study : Zipco Foods

# In[ ]:


display(Image("dataarchitecture.png"))    
# DataLink = "https://drive.google.com/file/d/1m-S4FMrblJ6J2dItKRl2h-S9zMyO4u-G/view?usp=drive_link"


# In[74]:


import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
from io import StringIO, BytesIO
import os
from IPython.display import Image, display
import warnings


# ## Data Extraction

# In[48]:


try:
    df = pd.read_csv(r'zipco_transaction.csv')
    print("Data loaded successfully!")
    print(f"Current shape: {df.shape}")
except Exception as e:
    print(f"An error occurred: {e}")


# In[49]:


df.head()


# In[50]:


df.info()


# ## Data cleaning and transformation

# In[56]:


# Remove duplicates
# Drop duplicate rows from the DataFrame
data = df.copy()
print(f"Initial shape: {data.shape}")
print("Removing duplicates...")
data.drop_duplicates(inplace=True)
print(f" Duplicates removed. Current shape: {data.shape}")


# In[62]:


# Convert object columns that look like dates to datetime
with warnings.catch_warnings():
    warnings.simplefilter("ignore", UserWarning)

    for col in data.select_dtypes(include='object').columns:
        try:
            converted = pd.to_datetime(data[col], errors='raise')
            if converted.notna().sum() > 0:
                data[col] = converted
                print(f"✅ Converted '{col}' to datetime.")
        except Exception:
            pass  # Skip columns that can't be converted


# In[63]:


data.info()


# In[64]:


# Handling missing values (Example: fill missing numeric values with the mean or median)
numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
for col in numeric_columns:
    #data[col].fillna(data[col].mean(), inplace=True)
    mean_value = data[col].mean()
    data.fillna({col: data[col].mean()}, inplace=True)
    print(f" Filled missing values in '{col}' with mean: {mean_value:.2f}")

print(" All numeric missing values filled.")


# In[65]:


# Handling missing values (Example: fill missing string values with 'Unknown')
string_columns = data.select_dtypes(include=['object']).columns
for col in string_columns:
    #data[col].fillna('Unknown', inplace=True)
    data.fillna({col: 'Unknown'}, inplace=True)
    print(f" Filled missing values in '{col}' with unknown")
print(" All string missing values filled.")


# In[66]:


data.info()


# In[67]:


# Create Products Table
products = data[['ProductName', 'UnitPrice']].drop_duplicates().reset_index(drop=True)
products.index.name = 'ProductID'
products = products.reset_index()
products.head()


# In[68]:


# Create Customers Table
customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
customers.index.name = 'CustomerID'
customers = customers.reset_index()
customers.head()  


# In[69]:


# Create Staff Table
staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
staff.index.name = 'StaffID'
staff = staff.reset_index()
staff.head()


# In[70]:


# Create Transaction Table
transactions = data.merge(products, on = ['ProductName', 'UnitPrice'], how='left') \
                   .merge(customers, on = ['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail'], how='left') \
                   .merge(staff, on= ['Staff_Name', 'Staff_Email'], how='left')
transactions.index.name = 'TransactionID'
transactions = transactions.reset_index() \
                           [['TransactionID', 'Date', 'ProductID', 'CustomerID', 'StaffID', 'Quantity', 'StoreLocation', 'PaymentType', \
                                'PromotionApplied', 'Weather', 'Temperature', 'StaffPerformanceRating', 'CustomerFeedback', \
                                'DeliveryTime_min', 'OrderType', 'DayOfWeek', 'TotalSales']]


# In[71]:


transactions.head()


# ## Data Loading: Save normalized tables to azure container

# In[1]:


from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


# --Save normalized tables to new CSV files
# data.to_csv('data/clean_data.csv', index=False)
# products.to_csv('data/products.csv', index=False)
# customers.to_csv('data/customers.csv', index=False)
# staff.to_csv('data/staff.csv', index=False)
# transactions.to_csv('data/transactions.csv', index=False)

# In[81]:


# Azure connection string and container name
load_dotenv()
connect_str = os.getenv('ZIPCO_AZURE_STORAGE_CONNECTION_STRING')
container_name = os.getenv('zipco_container_name')

# Create a BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)


# In[ ]:


# Dictionary of DataFrames
dfs = {
     "cleaned_data": data,
      "products": products,
      "customers": customers,
      "staff": staff,
      "transactions": transactions
}


# In[82]:


# Upload Multiple DataFrames as CSVs In-Memory
for name, df in dfs.items():
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{name}.csv")
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
    print(f" Uploaded {name}.csv into Azure Blob Storage")


# In[80]:


# Alternative: Upload Multiple DataFrames as Parquet Files In-Memory
for name, df in dfs.items():
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{name}.parquet")
    blob_client.upload_blob(buffer, overwrite=True)
    print(f" Uploaded {name}.parquet into Azure Blob Storage")

