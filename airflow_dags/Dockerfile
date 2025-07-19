FROM apache/airflow:2.8.0-python3.9

USER root

COPY requirements.txt /

# Use the `airflow` user to install packages
USER airflow

RUN pip install --no-cache-dir -r /requirements.txt

COPY . .
