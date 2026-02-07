# importações do pyspark e outras necessárias para fazer a transformação dos dados do bronze para o silver
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, upper, trim

# Criar uma sessão do Spark
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# 1. Procurar pelo nosso .csv do bronze e ler usando ele o spark.read.csv() para criar um dataframe do bronze
bronze_df = spark.read.csv(
    "data/bronze-layer/PIB_dos_Municipios_2010_2021.csv", header=True, inferSchema=True
)

# 1.5 refinar os dados
bronze_df = bronze_df.select(
    *[col(c).alias(c.strip().lower().replace(" ", "_").replace(",", ""))
    for c in bronze_df.columns]
)

# 2. Fazer as transformações necessárias para limpar os dados e deixá-los prontos para o silver
# Transformações necessárias:
# - Renomear as colunas para facilitar o entendimento
# - Converter a coluna de data para o formato de data do Spark
silver_df = (
    bronze_df
    .withColumnRenamed("ano", "ano")
    .withColumnRenamed("nome_do_município", "municipio")
    .withColumnRenamed(
        "valor_adicionado_bruto_da_agropecuária",
        "pib"
    )
    .withColumn("ano", col("ano").cast("int"))
    .withColumn("municipio", upper(trim(col("municipio"))))
    .withColumn("pib", col("pib").cast("double"))
    .filter(col("pib") >= 0)
    .groupBy("municipio", "ano")
    .agg(spark_sum("pib").alias("pib"))
)

# 3. Escrever o dataframe do silver usando o spark.write.parquet() para salvar
silver_df.write.mode("overwrite").parquet(
    "data/silver-layer/pib_municipios.parquet"
)

# Parar a sessão do Spark
spark.stop()
