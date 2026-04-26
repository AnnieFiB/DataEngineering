# Data Engineering Stack Portfolio

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python)](https://www.python.org/)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apache-airflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?logo=postgresql&logoColor=white)
![API](https://img.shields.io/badge/API-FF6F00?logo=fastapi&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-2C2D72?logo=apache-spark&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-0078D4?logo=microsoft-azure&logoColor=white)
[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-latest-orange?logo=apache&logoColor=white)](https://beam.apache.org/)
[![Google Cloud](https://img.shields.io/badge/Google%20Cloud-4285F4?logo=googlecloud&logoColor=white)](https://cloud.google.com/)
[![AWS](https://img.shields.io/badge/Amazon%20AWS-232F3E?logo=amazonaws&logoColor=white)](https://aws.amazon.com/)

## Repository Structure

```
/
│──DataEngineering/
     |
     │── airflow_dags/     # Apache Airflow DAGs & scheduling workflows
     │   ├── app/
     │   ├── dags/
     │   ├── Dockerfile
     │   └── requirements.txt
     │── pyspark/                   # PySpark jobs and transformation logic
     │── DWHmodelling/              # Database design & data warehouse schemas
     │── projects_python_scripts/   # Python scripts for ETL/data ops
     │── API_WebScr/           # APIs, crawling and data collection scripts
     │   └── Dockerfile
     │── assets/                    # Images, diagrams, or templates
     ├── .gitignore
     ├── cleanup.bat
     ├── requirements.txt
     ├── docker-compose.yml         # Master Compose file
     └── README.md


```

## Key Components

### 1. Data Engineering

 | Feature              | Description                                                   |
|----------------------|---------------------------------------------------------------|
| **Data Models**      | Database and data warehouse schema design (star/snowflake)    |
| **Airflow**          | DAG-based orchestration and task scheduling                   |
| **Spark**            | Distributed data processing and transformation                |
| **Data Quality**     | Great Expectations for rule-based validation and profiling    |
| **APIs & Webscraping**| Collecting structured/unstructured data from web & endpoints |


## Workflow Example

```bash
# 1. 
python DataEngineering/pipelines/data_cleaning.py

## Maintenance

```bash
# Run cleanup script (Windows)
cleanup.bat
cleanup.sh

```
