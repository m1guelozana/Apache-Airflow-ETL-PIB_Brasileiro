from pyspark.sql import SparkSession


def gold_to_dw():
    # Adicionamos as extensões do Delta aqui também!
    spark = (
        SparkSession.builder.appName("GoldToDW")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    jdbc_url = "jdbc:postgresql://postgres-dw:5432/dw_pib"

    tables = [
        "crescimento_pib",
        "pib_por_municipio",
        "pib_total_por_ano",
        "top_municipios_por_ano",
    ]

    for table in tables:
        try:
            path = f"/opt/airflow/data/gold-layer/{table}"
            df = spark.read.format("delta").load(path)

            df.write.format("jdbc").option("url", jdbc_url).option(
                "dbtable", table
            ).option("user", "airflow").option("password", "airflow").option(
                "driver", "org.postgresql.Driver"
            ).mode("overwrite").save()
            print(f"Sucesso: {table}")
        except Exception as e:
            print(f"Erro ao processar {table}: {e}")
            raise e  # Isso faz o Airflow mostrar o erro real no log

    spark.stop()


if __name__ == "__main__":
    gold_to_dw()
