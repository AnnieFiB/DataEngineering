# ==================================================================================
# # Example Usage of Enhanced Loading Script
# ## Demonstrates both Azure Blob Storage and Simplified Database loading capabilities
# ==================================================================================

import os
import sys
from dotenv import load_dotenv, find_dotenv
import loading
import transformation

# Load environment variables
load_dotenv(find_dotenv())

def example_azure_load():
    """
    Example of how to use the existing Azure blob storage loading functionality.
    This preserves your existing pipeline functionality.
    """
    print("🚀 Example: Azure Blob Storage Loading (Existing Pipeline)")
    print("=" * 60)
    
    # Mock transformed data (in real scenario, this comes from transformation step)
    mock_transformed_data = {
        'dim_customer': None,  # Would contain actual DataFrame
        'dim_vehicle': None,   # Would contain actual DataFrame
        'dim_location': None,  # Would contain actual DataFrame
        'dim_payment': None,   # Would contain actual DataFrame
        'dim_reason': None,    # Would contain actual DataFrame
        'dim_date': None,      # Would contain actual DataFrame
        'fact_booking': None,  # Would contain actual DataFrame
        'cleaned_data': None   # Would contain actual DataFrame
    }
    
    container_name = os.getenv('uber_container_name', 'uber-data')
    out_prefix = os.getenv('output_prefix', 'transformed-data')
    
    try:
        print("📤 Starting Azure upload process...")
        results = loading.load_to_azure(mock_transformed_data, container_name, out_prefix)
        
        print("\n📋 Azure Upload Results:")
        for table_name, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"  {table_name}: {status}")
            
    except Exception as e:
        print(f"❌ Azure loading failed: {e}")


def example_database_load():
    """
    Example of how to use the NEW simplified database loading functionality.
    This adds database loading capabilities to your existing pipeline.
    """
    print("\n🚀 Example: Simplified Database Loading (NEW)")
    print("=" * 60)
    
    # Step 1: Simulate getting transformed data (in real scenario, this comes from transformation step)
    print("\n📊 Step 1: Getting transformed data...")
    
    # This would normally come from your transformation step
    # For demonstration, we'll create a mock transformed_data structure
    mock_transformed_data = {
        'dim_customer': None,  # Would contain actual DataFrame
        'dim_vehicle': None,   # Would contain actual DataFrame
        'dim_location': None,  # Would contain actual DataFrame
        'dim_payment': None,   # Would contain actual DataFrame
        'dim_reason': None,    # Would contain actual DataFrame
        'dim_date': None,      # Would contain actual DataFrame
        'fact_booking': None,  # Would contain actual DataFrame
        'cleaned_data': None   # Would contain actual DataFrame
    }
    
    print("✅ Mock transformed data structure created")
    
    # Step 2: Configure database connection
    print("\n🔧 Step 2: Configuring database connection...")
    
    # Option A: Use default configuration from environment variables
    db_config = None  # Will use DB_CONFIG from loading.py
    
    # Option B: Override configuration
    # db_config = {
    #     'host': 'localhost',
    #     'port': '5432',
    #     'database': 'uber_dw',
    #     'user': 'postgres',
    #     'password': 'your_password',
    #     'schema': 'olap'
    # }
    
    print("✅ Database configuration ready")
    
    # Step 3: Load data to database
    print("\n💾 Step 3: Loading data to database...")
    
    try:
        # Use the simplified database loading function
        results = loading.load_to_database(mock_transformed_data, db_config)
        
        print("\n📋 Database Load Results:")
        for table_name, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"  {table_name}: {status}")
            
    except Exception as e:
        print(f"❌ Database loading failed: {e}")
    
    print("\n" + "=" * 60)
    print("🎯 Simplified database loading example completed!")


def example_combined_pipeline():
    """
    Example of how to use BOTH Azure and Database loading in the same pipeline.
    This shows how to extend your existing Azure pipeline with database capabilities.
    """
    print("\n🚀 Example: Combined Azure + Database Pipeline")
    print("=" * 60)
    
    # Mock transformed data
    mock_transformed_data = {
        'dim_customer': None,
        'dim_vehicle': None,
        'fact_booking': None
    }
    
    try:
        print("🔄 Step 1: Loading to Azure Blob Storage...")
        container_name = os.getenv('uber_container_name', 'uber-data')
        out_prefix = os.getenv('output_prefix', 'transformed-data')
        
        azure_results = loading.load_to_azure(mock_transformed_data, container_name, out_prefix)
        print("✅ Azure loading completed")
        
        print("\n🔄 Step 2: Loading to PostgreSQL Database...")
        db_results = loading.load_to_database(mock_transformed_data)
        print("✅ Database loading completed")
        
        print("\n📋 Combined Pipeline Results:")
        print("Azure Results:")
        for table_name, success in azure_results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"  {table_name}: {status}")
        
        print("\nDatabase Results:")
        for table_name, success in db_results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"  {table_name}: {status}")
            
    except Exception as e:
        print(f"❌ Combined pipeline failed: {e}")


def example_simple_database_functions():
    """
    Example of how to use the simplified database loading functions directly.
    """
    print("\n🚀 Example: Simple Database Functions")
    print("=" * 60)
    
    print("📚 Available simple database functions:")
    print("  - loading.load_to_database() - Main database loading function")
    print("  - loading.load_to_database_simple() - Direct simple function")
    print("  - loading.load_transformed_data_to_db() - Alternative function name")
    print("  - loading.create_simple_table() - Create individual tables")
    print("  - loading.upsert_from_df() - Upsert individual DataFrames")
    
    print("\n✅ All functions are simple and easy to use")


def example_legacy_functions():
    """
    Example of how to use the existing legacy functions.
    These are preserved for backward compatibility.
    """
    print("\n🚀 Example: Legacy Functions (Backward Compatibility)")
    print("=" * 60)
    
    print("📚 Available legacy functions:")
    print("  - loading.load_to_azure() - Your existing Azure pipeline")
    print("  - loading.check_and_create_db() - Database creation utility")
    print("  - loading.upsert_from_df() - Individual table upsert")
    print("  - loading.load_all_known_tables() - Legacy table loading")
    
    print("\n✅ All legacy functions are preserved and functional")


if __name__ == "__main__":
    print("🎯 Uber ETL Pipeline - Enhanced Loading Examples")
    print("=" * 60)
    print("💡 This script demonstrates both existing and new functionality")
    print("   - Azure Blob Storage (existing pipeline)")
    print("   - PostgreSQL Database (simplified new capability)")
    print("   - Combined pipeline usage")
    print("=" * 60)
    
    # Run examples
    example_azure_load()           # Your existing Azure functionality
    example_database_load()        # New simplified database functionality
    example_combined_pipeline()    # Using both together
    example_simple_database_functions()  # Simple database functions
    example_legacy_functions()     # Backward compatibility
    
    print("\n" + "=" * 60)
    print("🎉 All examples completed!")
    print("\n💡 Key Benefits:")
    print("  1. ✅ Your existing Azure pipeline is preserved")
    print("  2. ✅ New simplified database loading capability added")
    print("  3. ✅ Can use both or either independently")
    print("  4. ✅ Backward compatibility maintained")
    print("  5. ✅ Simple and easy to understand functions")
    print("\n🚀 To use in production:")
    print("  - Azure only: loading.load_to_azure(transformed_data, container, prefix)")
    print("  - Database only: loading.load_to_database(transformed_data)")
    print("  - Both: Call both functions in sequence")
    print("\n✨ Database loading is now simple:")
    print("  - Just call loading.load_to_database(transformed_data)")
    print("  - Tables are created automatically")
    print("  - Uses first column as conflict key")
    print("  - No complex configuration needed")
