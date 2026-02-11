from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PIB Delta Job")
    .getOrCreate()
)

df = spark.createDataFrame(
    [(2022, "SP", 3000), (2022, "RJ", 1800)],
    ["ano", "uf", "pib"]
)

df.write.format("delta").mode("overwrite").save("/data/delta/pib")

spark.stop()
