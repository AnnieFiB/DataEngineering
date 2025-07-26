import pandas as pd
import warnings

def transformation(data: pd.DataFrame):
    data.drop_duplicates(inplace=True)

    numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
    data[numeric_columns] = data[numeric_columns].fillna(data[numeric_columns].mean())

    string_columns = data.select_dtypes(include=['object']).columns
    data[string_columns] = data[string_columns].fillna('Unknown')
    
    # Convert object columns that look like dates to datetime
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        for col in data.select_dtypes(include='object').columns:
            try:
                converted = pd.to_datetime(data[col], errors='raise')
                if converted.notna().sum() > 0:
                    data[col] = converted
                    print(f"Converted '{col}' to datetime.")
            except Exception:
                pass

    # Create normalized tables
    products = data[['ProductName', 'UnitPrice']].drop_duplicates().reset_index(drop=True)
    products.index.name = 'ProductID'
    products.reset_index(inplace=True)

    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers.index.name = 'CustomerID'
    customers.reset_index(inplace=True)

    staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    staff.index.name = 'StaffID'
    staff.reset_index(inplace=True)

    transactions = (
        data
        .merge(products, on=['ProductName', 'UnitPrice'], how='left')
        .merge(customers, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail'], how='left')
        .merge(staff, on=['Staff_Name', 'Staff_Email'], how='left')
    )

    transactions.index.name = 'TransactionID'
    transactions.reset_index(inplace=True)
    transactions = transactions[[
        'TransactionID', 'Date', 'ProductID', 'CustomerID', 'StaffID',
        'Quantity', 'StoreLocation', 'PaymentType', 'PromotionApplied', 'Weather',
        'Temperature', 'StaffPerformanceRating', 'CustomerFeedback',
        'DeliveryTime_min', 'OrderType', 'DayOfWeek', 'TotalSales'
    ]]

    print("Transformation successful.")
    return data, products, customers, staff, transactions


