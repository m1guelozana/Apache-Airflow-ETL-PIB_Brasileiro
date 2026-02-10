FROM apache/airflow:2.8.0-python3.11

USER airflow

RUN pip install apache-airflow-providers-apache-spark
