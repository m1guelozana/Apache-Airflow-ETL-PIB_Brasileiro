# importações do pyspark e outras necessárias para fazer a transformação dos dados do bronze para o silver
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Criar uma sessão do Spark
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# 1. Procurar pelo nosso .csv do bronze e ler usando ele o spark.read.csv() para criar um dataframe do bronze
bronze_df = spark.read.csv(
    "data/bronze-layer/PIB_dos_Municipios_2010_2021.csv", header=True, inferSchema=True
)

# 2. Fazer as transformações necessárias para limpar os dados e deixá-los prontos para o silver
# Transformações necessárias:
# - Renomear as colunas para facilitar o entendimento
# - Converter a coluna de data para o formato de data do Spark
silver_df = bronze_df.select(
    to_date(col("Ano").cast("string"), "yyyy").alias("ano"),
    col("Nome do Município").alias("municipio"),
    col("Valor adicionado bruto da Agropecuária, ").alias("valor_agro")
)

# 3. Escrever o dataframe do silver usando o spark.write.parquet() para salvar
silver_df.write.mode("overwrite").parquet(
    "data/silver-layer/valor_agro_municipios.parquet"
)

# Parar a sessão do Spark
spark.stop()
