# Importações necessárias para fazer a transformação dos dados do silver para o gold
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from delta import configure_spark_with_delta_pip

# Criar uma sessão do Spark
builder = (
    SparkSession.builder
    .appName("SilverToGold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 1. Procurar pelo nosso .parquet do silver e ler usando ele o spark.read.parquet() para criar um dataframe do silver
silver_df = spark.read.parquet(
    "data/silver-layer/valor_agro_municipios.parquet"
)

# 2. Fazer as transformações necessárias para agregar os dados por ano e calcular o PIB total do Brasil para cada ano
# Printar o schema do silver_df para entender a estrutura dos dados
silver_df.printSchema()

gold_df = (
    silver_df
    .groupBy("ano")
    .agg(spark_sum("valor_agro").alias("valor_agro_brasil"))
    .orderBy(col("ano"))
)

# 3. Escrever o dataframe do gold, transformando ele em um delta file usando o spark.write.format("delta").save() para salvar

gold_df = (
    silver_df
    .groupBy("ano")
    .agg(spark_sum("valor_agro").alias("valor_agro_brasil"))
    .orderBy(col("ano"))
)

# 4. Subir o delta file para o postgres usando o spark.write.format("jdbc").options() para configurar a conexão com o banco de dados e salvar os dados na tabela do postgres

# 5. Ler o delta file do gold usando o spark.read.format("delta").load() para verificar se os dados foram salvos corretamente
gold_df.write.format("delta").mode("overwrite").save(
    "data/gold-layer/valor_agro_brasil"
)

# 6. Parar a sessão do Spark
spark.stop()