# 🏦 Modern Banking Data Stack: Real-Time CDC Pipeline

## 📖 Project Overview
This project implements a real-time banking data platform. It captures transactional changes from a production database and streams them through a modern data lake into a cloud warehouse for analytics.

## 🏗️ Architecture
The pipeline follows a **Change Data Capture (CDC)** pattern:
1. **Source:** Postgres (Transactional Database)
2. **Streaming:** Debezium captures Row-level changes and streams them to **Kafka**.
3. **Data Lake:** A Python consumer writes Kafka events to **MinIO** in Parquet format.
4. **Orchestration:** **Airflow** triggers the ingestion from MinIO to **Snowflake**.
5. **Transformation:** **dbt** models the data into a Medallion Architecture (Bronze/Silver/Gold).



## 🛠️ Tech Stack
* **Storage:** Postgres, MinIO (S3-compatible), Snowflake
* **Streaming:** Kafka, Debezium, Confluent Schema Registry
* **Workflow:** Apache Airflow, Docker, dbt