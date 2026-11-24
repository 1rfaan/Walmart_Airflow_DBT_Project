FROM apache/airflow:3.2.0

USER root

RUN apt-get update && apt-get install -y gcc && apt-get clean

USER airflow

Copy requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt