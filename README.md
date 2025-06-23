# Data Engineering Stack Portfolio

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python)](https://www.python.org/)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apache-airflow&logoColor=white)
![Spark](https://img.shields.io/badge/Spark-E25A1C?logo=apachespark&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?logo=postgresql&logoColor=white)
![Great Expectations](https://img.shields.io/badge/GreatExpectations-000000?logo=python&label=Great%20Expectations&logoColor=white)
![API](https://img.shields.io/badge/API-FF6F00?logo=fastapi&logoColor=white)


## Repository Structure

```
/
├── DataEngineering/
│   ├── airflow_taskScheduler/     # Apache Airflow DAGs & scheduling workflows
│   ├── pyspark/                   # PySpark jobs and transformation logic
│   ├── DWHmodelling/              # Database design & data warehouse schemas
│   ├── projects_python_scripts/   # Python scripts for ETL/data ops
│   ├── api_webscraping/           # APIs, crawling and data collection scripts
│   └── assets/                    # Images, diagrams, or templates
├── .gitignore
├── cleanup.bat
├── requirements.txt
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