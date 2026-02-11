FROM apache/airflow:2.8.0-python3.11

USER root

# Instala o Java (necessário para o SparkSubmitOperator falar com o Spark)
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

# Instala as dependências de Python de uma vez
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install --no-cache-dir apache-airflow-providers-apache-spark