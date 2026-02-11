# 📊 PIB Municipal Data Pipeline

Pipeline de dados construído com **Spark + Airflow + Delta Lake + Metabase**, seguindo arquitetura em camadas (Bronze → Silver → Gold).

---

# 🏗️ Arquitetura do Projeto

## 📐 Desenho da Arquitetura

> Substitua o link abaixo pelo link da imagem no Imgur

![Arquitetura do Projeto](https://imgur.com/undefined)

---

## 🔎 Explicação da Arquitetura

O projeto segue o padrão **Medallion Architecture**, dividido em três camadas:

### 🥉 Bronze Layer
- Recebe os dados brutos
- Sem transformações significativas
- Armazenamento inicial no Data Lake

### 🥈 Silver Layer
- Limpeza e padronização
- Tratamento de tipos
- Remoção de inconsistências
- Estruturação intermediária

### 🥇 Gold Layer
- Dados agregados
- Modelagem voltada para análise
- Estrutura otimizada para BI

---

## 🔄 Orquestração
![Arquitetura da Orquestração]((https://imgur.com/O3g1JBz))

A orquestração é feita pelo **Apache Airflow**, que:

- Executa o job `bronze_to_silver`
- Executa o job `silver_to_gold`
- Controla dependências
- Permite reprocessamento
- Mantém histórico de execuções

---

## 📊 Metabase
![Metabase_um](https://imgur.com/g6yZ3k9)
![Metabase_um](https://imgur.com/LB976HG)
![Metabase_um](https://imgur.com/gGD4W4U)
![Metabase_um](https://imgur.com/gGD4W4U)
![Metabase_um](https://imgur.com/7USEGAx)

---

# ⚙️ Stack Utilizada

| Tecnologia | Função |
|------------|--------|
| Apache Spark | Processamento distribuído |
| Delta Lake | Armazenamento transacional |
| Apache Airflow | Orquestração |
| Docker | Containerização |
| Metabase | Visualização de dados |
| SQLite / Postgres | Metadata do Airflow |

---

# 🐳 Como Executar o Projeto

## 1️⃣ Subir containers

```bash
docker compose build --no-cache
docker compose up -d
