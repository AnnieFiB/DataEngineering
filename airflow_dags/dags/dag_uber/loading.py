# ==================================================================================
# # Batch ETL Pipeline (Cloud deployment)
# ## Case Study - Azure Blob Storage, Azure Databricks, synapse, data factory
# ==================================================================================
# ### Libraries and Dependencies

import pandas as pd
import os
import sys
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import psycopg2.extensions
from psycopg2 import sql
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
psycopg2.extensions.register_adapter(np.float64, psycopg2._psycopg.AsIs)


from dotenv import find_dotenv, load_dotenv
import extraction
import transformation


load_dotenv(find_dotenv())

# Azure Configuration
connect_str = os.getenv('UBER_STORAGE_CONNECTION_STRING')
container_name = os.getenv('uber_container_name')
out_prefix = os.getenv('output_prefix', 'transformed-data')

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('UBER_DB_HOST', 'localhost'),
    'port': os.getenv('UBER_DB_PORT', '5432'),
    'database': os.getenv('UBER_DB_NAME', 'uber_dw'),
    'user': os.getenv('UBER_DB_USER', 'postgres'),
    'password': os.getenv('UBER_DB_PASSWORD'),
    'schema': os.getenv('UBER_DB_SCHEMA', 'olap')
}

# ==========================
# Load to Azure Bucket
# ==========================

def create_blob_mapping(df_name, out_prefix):
    """
    Dynamically create blob path for given dataframe name.
    
    Args:
        df_name (str): Name of the dataframe (e.g., 'cleaned_data', 'dim_customer')
        out_prefix (str): Output prefix path
    
    Returns:
        str: Full blob path
    """
    if df_name == 'cleaned_data':
        return f"{out_prefix}/cleaned/uber_cleaned_data.parquet"
    elif df_name.startswith('dim_'):
        return f"{out_prefix}/dimensions/{df_name}.parquet"
    elif df_name.startswith('fact_'):
        return f"{out_prefix}/facts/{df_name}.parquet"
    else:
        return f"{out_prefix}/other/{df_name}.parquet"

def load_to_azure(transformed_data, container_name, out_prefix):
    """
    Load transformed data to Azure Blob Storage.
    
    Args:
        transformed_data (dict): Dictionary containing dataframes from transformation
        container_name (str): Azure container name
        out_prefix (str): Output prefix path
    """
    print("Starting Azure upload process...")
    
    for df_name, df in transformed_data.items():
        if df is not None and not df.empty:
            try:
               
                blob_path = create_blob_mapping(df_name, out_prefix)                
                print(f"Uploading {df_name} ({len(df)} rows) to {blob_path}")
                
                extraction.upload_dataframe_to_blob(df, container_name, blob_path)
                
                print(f"Successfully uploaded {df_name}")
                
            except Exception as e:
                print(f"Failed to upload {df_name}: {str(e)}")
        else:
            print(f"Skipping {df_name} - no data or empty dataframe")
    
    print("Azure upload process completed!")


# ==================================================================================
# # PostgreSQL Database Loading
# ==================================================================================
def create_simple_table(conn, df, table_name, schema='olap'):
    """
    Create a simple table based on DataFrame structure.
    
    Args:
        conn: Database connection
        df: DataFrame to analyze
        table_name: Name of the table
        schema: Database schema
    """
    try:
        with conn.cursor() as cur:
            # Create schema if it doesn't exist
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            
            # Generate simple column definitions
            columns = []
            for col_name, dtype in df.dtypes.items():
                if 'int' in str(dtype):
                    pg_type = 'BIGINT'
                elif 'float' in str(dtype):
                    pg_type = 'DOUBLE PRECISION'
                elif 'datetime' in str(dtype):
                    pg_type = 'TIMESTAMP'
                elif 'bool' in str(dtype):
                    pg_type = 'BOOLEAN'
                else:
                    pg_type = 'TEXT'
                columns.append(f"{col_name} {pg_type}")
            
            # Create table
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    {', '.join(columns)}
                );
            """
            cur.execute(create_sql)
            conn.commit()
            print(f"✅ Table {schema}.{table_name} created/verified")
            
    except Exception as e:
        print(f"❌ Failed to create table {schema}.{table_name}: {e}")
        conn.rollback()

def upsert_from_df(conn, df, table_name, conflict_columns, update_columns=None, schema =None):
    """
    Upserts a DataFrame into a PostgreSQL table with debug status output.

    Parameters:
    - conn: psycopg2 connection object
    - df: pandas DataFrame to upsert
    - table_name: name of the target table
    - conflict_columns: list of columns to use for ON CONFLICT clause
    - update_columns: list of columns to update on conflict; if None, all except conflict_columns
    - schema: database schema (default is 'public')
    """
    if df is None or df.empty:
        print(f"Skipping {schema}.{table_name}: DataFrame is empty or None...")
        return

    print(f"\n Preparing to upsert {len(df)} rows into {schema}.{table_name}...")

    if update_columns is None:
        update_columns = [col for col in df.columns if col not in conflict_columns]

    columns = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]
    placeholders = ', '.join(columns)
    conflict_cols = ', '.join(conflict_columns)
    update_stmt = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])

    insert_sql = (
        f"INSERT INTO {schema}.{table_name} ({placeholders})\n"
        f"VALUES %s\n"
        f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_stmt};"
    )

    # Ensure the connection is in autocommit mode
    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
        conn.commit()
        print(f"✅ {len(df)} records upserted into {schema}.{table_name}")
    except Exception as e:
        print(f"❌ Failed to upsert into {schema}.{table_name}: {e}")
        conn.rollback()

def load_to_database(transformed_data, db_config=None):
    """
    Function to load transformed data to PostgreSQL database.
    
    Args:
        transformed_data (dict): Dictionary containing transformed DataFrames
        db_config (dict): Optional database configuration override
        
    Returns:
        dict: Results of database load operations
    """
    # Use default config if none provided
    config = db_config if db_config else DB_CONFIG
     
    try:
        # Connect to database
        print(f"Attempting to connect to database: {config['database']} at {config['host']}:{config['port']}")
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        conn.autocommit = False
        
        print(f"Connected to database: {config['database']}")
        
        results = {}
        
        # Process each table
        for table_name, df in transformed_data.items():
            if df is not None and not df.empty:
                try:
                    # Create table
                    create_simple_table(conn, df, table_name, config['schema'])
                    
                    # Simple upsert - use first column as conflict key
                    conflict_col = df.columns[0]
                    upsert_from_df(conn, df, table_name, [conflict_col], schema=config['schema'])
                    
                    results[table_name] = True
                    print(f"{table_name} loaded successfully with {len(df)} rows of data" )
                    
                except Exception as e:
                    print(f" Failed to load {table_name}: {e}")
                    results[table_name] = False
            else:
                print(f"Skipping {table_name}: No data")
                results[table_name] = True
        
        conn.close()
        print("Database loading completed!")
        return results
        
    except Exception as e:
        print(f" Database connection failed: {e}")
        return {"status": "error", "error": str(e)}

# =======================
# Final loading script
# ========================
def load_transformed_data_to_gcs_db(transformed_data, container_name, out_prefix, db_config=None):
    """
    Alternative function for loading transformed data to azure bucket and database.
    
    Args:
        transformed_data (dict): Dictionary containing transformed DataFrames
        db_config (dict): Optional database configuration override
        container_name: Azure container name
        out_prefix: Output prefix path
        
    Returns:
        dict: Results of both Azure and database load operations
    """
    try:
        # Load to Azure
        print("Loading to Azure...")
        load_to_azure(transformed_data, container_name, out_prefix)
        
        # Load to Database
        print("Loading to Database...")
        db_result = load_to_database(transformed_data, db_config)
        
        # Return combined results
        result = db_result
        print(f"Combined loading completed: {result}")
        return result
        
    except Exception as e:
        print(f"Combined loading failed: {e}")
        raise


