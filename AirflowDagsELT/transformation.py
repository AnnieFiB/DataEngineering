import pandas as pd


def transformation():
    data = pd.read_csv(r'zipco_transaction.csv')
    # Remove duplicates
    data.drop_duplicates(inplace=True)

    # Handling missing values (Example: fill missing numeric values with the mean or median)
    numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        #data[col].fillna(data[col].mean(), inplace=True)
        data.fillna({col: data[col].mean()}, inplace=True)

    # Handling missing values (Example: fill missing string values with 'Unknown')
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        #data[col].fillna('Unknown', inplace=True)
        data.fillna({col: 'Unknown'}, inplace=True)

    # Create Products Table
    products = data[['ProductName', 'UnitPrice']].drop_duplicates().reset_index(drop=True)
    products.index.name = 'ProductID'
    products = products.reset_index()

    # Create Customers Table
    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers.index.name = 'CustomerID'
    customers = customers.reset_index()

    # Create Staff Table
    staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    staff.index.name = 'StaffID'
    staff = staff.reset_index()

    # Create Transaction Table
    transactions = data.merge(products, on = ['ProductName', 'UnitPrice'], how='left') \
                    .merge(customers, on = ['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail'], how='left') \
                    .merge(staff, on= ['Staff_Name', 'Staff_Email'], how='left')
    transactions.index.name = 'TransactionID'
    transactions = transactions.reset_index() \
                            [['TransactionID', 'Date', 'ProductID', 'CustomerID', 'StaffID', 'Quantity', 'StoreLocation', 'PaymentType', \
                                    'PromotionApplied', 'Weather', 'Temperature', 'StaffPerformanceRating', 'CustomerFeedback', \
                                    'DeliveryTime_min', 'OrderType', 'DayOfWeek', 'TotalSales']]
    
    # Save normalized tables to new CSV files
    data.to_csv('clean_data.csv', index=False)
    products.to_csv('products.csv', index=False)
    customers.to_csv('customers.csv', index=False)
    staff.to_csv('staff.csv', index=False)
    transactions.to_csv('transactions.csv', index=False)
    print('Normalised data saved successfully!')
