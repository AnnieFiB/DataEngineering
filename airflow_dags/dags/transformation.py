import pandas as pd
import warnings



def transformation(df):
    if df.empty:
        print("No data available for transformation.")
        return None

    # Remove duplicates
    data = df.copy()
    print(f"Initial shape: {data.shape}")
    print("Removing duplicates...")
    data.drop_duplicates(inplace=True)
    print(f" Duplicates removed. Current shape: {data.shape}")
    
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

    # Handling missing values (Example: fill missing numeric values with the mean or median)
    numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        #data[col].fillna(data[col].mean(), inplace=True)
        mean_value = data[col].mean()
        data.fillna({col: data[col].mean()}, inplace=True)
        print(f" Filled missing values in '{col}' with mean: {mean_value:.2f}")

    print(" All numeric missing values filled.")

    # Handling missing values (Example: fill missing string values with 'Unknown')
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        #data[col].fillna('Unknown', inplace=True)
        data.fillna({col: 'Unknown'}, inplace=True)
        print(f" Filled missing values in '{col}' with unknown")
    print(" All string missing values filled.")

    print("\n Normalizing data into separate tables...")
    # Create Products Table
    products = data[['ProductName', 'UnitPrice']].drop_duplicates().reset_index(drop=True)
    products.index.name = 'ProductID'
    products = products.reset_index()
    products.head(2)

    # Create Customers Table
    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers.index.name = 'CustomerID'
    customers = customers.reset_index()
    customers.head(2)

    # Create Staff Table
    staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    staff.index.name = 'StaffID'
    staff = staff.reset_index()
    staff.head(2)   

    # Create Transaction Table
    transactions = data.merge(products, on = ['ProductName', 'UnitPrice'], how='left') \
                    .merge(customers, on = ['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail'], how='left') \
                    .merge(staff, on= ['Staff_Name', 'Staff_Email'], how='left')
    transactions.index.name = 'TransactionID'
    transactions = transactions.reset_index() \
                            [['TransactionID', 'Date', 'ProductID', 'CustomerID', 'StaffID', 'Quantity', 'StoreLocation', 'PaymentType', \
                                    'PromotionApplied', 'Weather', 'Temperature', 'StaffPerformanceRating', 'CustomerFeedback', \
                                    'DeliveryTime_min', 'OrderType', 'DayOfWeek', 'TotalSales']]
    transactions.head(2)
    print("Data transformation completed successfully!")
    
    # Return the dictionary of DataFrames
    dfs = {
        "cleaned_data": data,
        "products": products,
        "customers": customers,
        "staff": staff,
        "transactions": transactions
    }
    print("DataFrames created and stored inside a dictionary successfully!")

    return dfs
    

