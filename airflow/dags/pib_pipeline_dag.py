from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="pib_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["spark", "delta", "pib"],  # ty:ignore[invalid-argument-type]
) as dag:

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/airflow/jobs/bronze_to_silver.py",
        conn_id="spark_default",
        verbose=True,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/airflow/jobs/silver_to_gold.py",
        conn_id="spark_default",
        verbose=True,
    )

    bronze_to_silver >> silver_to_gold
