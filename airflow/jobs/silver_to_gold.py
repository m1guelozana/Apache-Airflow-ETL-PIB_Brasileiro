# IMPORTAÇÕES NECESSÁRIAS
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum as spark_sum, desc, row_number, lag
from delta import configure_spark_with_delta_pip


class SilverToGoldJob:
    def __init__(self, silver_path: str, gold_base_path: str):
        self.silver_path = silver_path
        self.gold_base_path = gold_base_path
        self.spark = self._create_spark_session()

    # 1. CRIAR A SPARK SESSION COM SUPORTE A DELTA
    def _create_spark_session(self):
        builder = (
            SparkSession.builder
            .appName("SilverToGold")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        )

        return configure_spark_with_delta_pip(builder).getOrCreate()

    # 2. FUNÇÕES DE REFINAMENTO (MÉTRICAS GOLD)
    # 2.1 PIB total do Brasil por ano
    def gerar_pib_total_por_ano(self, df):
        return (
            df.groupBy("ano")
                .agg(spark_sum("pib").alias("pib_total_brasil"))
                .orderBy("ano")
        )

    # 2.2 PIB por município e ano (base para outras métricas)
    def gerar_pib_por_municipio(self, df):
        return (
            df.groupBy("municipio", "ano")
                .agg(spark_sum("pib").alias("pib_municipio"))
        )

    # 2.3 Top N municípios por PIB em cada ano
    def gerar_top_municipios(self, df, top_n=10):
        window = Window.partitionBy("ano").orderBy(desc("pib_municipio"))

        return (
            df.withColumn("rank", row_number().over(window))
                .filter(col("rank") <= top_n)
        )

    # 2.4 Crescimento percentual do PIB por município
    def gerar_crescimento(self, df):
        window = Window.partitionBy("municipio").orderBy("ano")

        return (
            df.withColumn(
                "pib_ano_anterior",
                lag("pib_municipio").over(window)
            )
            .withColumn(
                "crescimento_percentual",
                (
                    (col("pib_municipio") - col("pib_ano_anterior"))
                    / col("pib_ano_anterior")
                ) * 100
            )
            .filter(col("pib_ano_anterior") >= 0)
        )

    # 3. EXECUÇÃO DO PIPELINE SILVER → GOLD
    def run(self):

        # 3.1 Ler os dados da camada Silver (Parquet)
        silver_df = self.spark.read.parquet(self.silver_path)

        # 3.2 Gerar as métricas Gold
        pib_por_municipio = self.gerar_pib_por_municipio(silver_df)
        pib_total_ano = self.gerar_pib_total_por_ano(silver_df)
        top_municipios = self.gerar_top_municipios(pib_por_municipio)
        crescimento = self.gerar_crescimento(pib_por_municipio)

        # 4. ESCRITA DA CAMADA GOLD (DELTA LAKE)
        # 4.1 PIB por município e ano
        pib_por_municipio.write.format("delta").mode("overwrite").save(
            f"{self.gold_base_path}/pib_por_municipio"
        )

        # 4.2 PIB total do Brasil por ano
        pib_total_ano.write.format("delta").mode("overwrite").save(
            f"{self.gold_base_path}/pib_total_por_ano"
        )

        # 4.3 Top municípios por PIB em cada ano
        top_municipios.write.format("delta").mode("overwrite").save(
            f"{self.gold_base_path}/top_municipios_por_ano"
        )

        # 4.4 Crescimento percentual do PIB por município
        crescimento.write.format("delta").mode("overwrite").save(
            f"{self.gold_base_path}/crescimento_pib"
        )

        # 5. Encerrar a SparkSession
        self.spark.stop()


# 6. PONTO DE ENTRADA DO SCRIPT
if __name__ == "__main__":
    job = SilverToGoldJob(
        silver_path="/opt/airflow/data/silver-layer/pib_municipios.parquet",
        gold_base_path="/opt/airflow/data/gold-layer/"
    )

    job.run()
