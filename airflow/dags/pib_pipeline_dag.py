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
        packages="io.delta:delta-spark_2.12:3.1.0",
        verbose=True,
    )

    gold_to_dw = SparkSubmitOperator(
        task_id="gold_to_dw",
        application="/opt/airflow/jobs/gold_to_dw.py",
        conn_id="spark_default",
        conf={
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.5.0",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        verbose=True,
        dag=dag,
    )

    bronze_to_silver >> silver_to_gold >> gold_to_dw
