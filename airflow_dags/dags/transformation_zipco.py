import pandas as pd
import os

def transformation():
    base_path = os.path.join(os.getenv("AIRFLOW_HOME", "."), "dags")
    input_file = os.path.join(base_path, "zipco_transaction.csv")
    
    data = pd.read_csv(input_file)
    
    # Remove duplicates
    data.drop_duplicates(inplace=True)

    # Fill missing numeric values
    numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        data.fillna({col: data[col].mean()}, inplace=True)

    # Fill missing string values
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data.fillna({col: 'Unknown'}, inplace=True)

    # Normalize data
    products = data[['ProductName', 'UnitPrice']].drop_duplicates().reset_index(drop=True)
    products.index.name = 'ProductID'
    products = products.reset_index()

    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers.index.name = 'CustomerID'
    customers = customers.reset_index()

    staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    staff.index.name = 'StaffID'
    staff = staff.reset_index()

    transactions = data.merge(products, on=['ProductName', 'UnitPrice'], how='left') \
        .merge(customers, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail'], how='left') \
        .merge(staff, on=['Staff_Name', 'Staff_Email'], how='left')

    transactions.index.name = 'TransactionID'
    transactions = transactions.reset_index()[[
        'TransactionID', 'Date', 'ProductID', 'CustomerID', 'StaffID', 'Quantity', 'StoreLocation', 'PaymentType',
        'PromotionApplied', 'Weather', 'Temperature', 'StaffPerformanceRating', 'CustomerFeedback',
        'DeliveryTime_min', 'OrderType', 'DayOfWeek', 'TotalSales'
    ]]

    # Save normalized tables
    data.to_csv(os.path.join(base_path, 'clean_data.csv'), index=False)
    products.to_csv(os.path.join(base_path, 'products.csv'), index=False)
    customers.to_csv(os.path.join(base_path, 'customers.csv'), index=False)
    staff.to_csv(os.path.join(base_path, 'staff.csv'), index=False)
    transactions.to_csv(os.path.join(base_path, 'transactions.csv'), index=False)

    print("Normalized data saved successfully!")
