# Database Setup Guide for Uber ETL Pipeline

## 🔧 **Environment Variables Required**

Create a `.env` file in your project directory with these variables:

```bash
# PostgreSQL Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=uber_dw
DB_USER=postgres
DB_PASSWORD=your_actual_password_here
DB_SCHEMA=olap

# Azure Configuration (if using Azure)
UBER_STORAGE_CONNECTION_STRING=your_azure_connection_string
uber_container_name=your_container_name
output_prefix=transformed-data
```

## 🗄️ **PostgreSQL Setup Steps**

### **1. Install PostgreSQL (if not already installed)**
```bash
# Windows (using chocolatey)
choco install postgresql

# macOS (using homebrew)
brew install postgresql

# Ubuntu/Debian
sudo apt-get install postgresql postgresql-contrib
```

### **2. Start PostgreSQL Service**
```bash
# Windows
net start postgresql-x64-15

# macOS
brew services start postgresql

# Ubuntu/Debian
sudo systemctl start postgresql
```

### **3. Create Database and User**
```bash
# Connect to PostgreSQL as superuser
psql -U postgres

# Create database
CREATE DATABASE uber_dw;

# Create user (optional - you can use postgres user)
CREATE USER uber_user WITH PASSWORD 'your_password';

# Grant privileges
GRANT ALL PRIVILEGES ON DATABASE uber_dw TO uber_user;

# Exit
\q
```

### **4. Test Connection**
```bash
# Test with your credentials
psql -h localhost -U postgres -d uber_dw
# Enter password when prompted
```

## 🚀 **Quick Test**

Run the test script to verify your setup:

```bash
cd DataEngineering/airflow_dags/dags/dag_uber/
python test_loading.py
```

## 🔍 **Troubleshooting**

### **Common Issues:**

1. **"no password supplied"**
   - Set `DB_PASSWORD` environment variable
   - Check if password contains special characters

2. **"connection refused"**
   - PostgreSQL service not running
   - Wrong port number
   - Firewall blocking connection

3. **"authentication failed"**
   - Wrong username/password
   - User doesn't exist
   - Wrong database name

### **Check PostgreSQL Status:**
```bash
# Windows
sc query postgresql-x64-15

# macOS
brew services list | grep postgresql

# Ubuntu/Debian
sudo systemctl status postgresql
```

### **Check PostgreSQL Logs:**
```bash
# Windows: Check Event Viewer
# macOS: Check Console.app
# Ubuntu/Debian:
sudo tail -f /var/log/postgresql/postgresql-*.log
```

## 📝 **Airflow Configuration**

If running in Airflow, add these as Airflow connections or environment variables in your Airflow configuration.

