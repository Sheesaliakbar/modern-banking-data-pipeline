# 🏦 Modern Banking Data Stack — Real-Time CDC Pipeline

> An end-to-end real-time data pipeline that captures every banking transaction the moment it happens — from PostgreSQL to Snowflake using Change Data Capture (CDC), Apache Kafka, and dbt.

[![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white)](https://snowflake.com)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)

---

## 🗺️ Interactive Architecture

👉 [View Live Architecture Diagram](https://sheesaliakbar.github.io/modern-banking-data-pipeline/banking-pipeline.html)

---

## 📖 Project Overview

Most data pipelines rely on batch processing — pulling data every hour or every night. This project is different.

This pipeline uses **Change Data Capture (CDC)** to detect and stream every `INSERT`, `UPDATE`, and `DELETE` from a production-like banking database in **real-time** — with millisecond latency — all the way through to a cloud data warehouse ready for analytics.

The full stack is containerized with **Docker Compose** and can be launched with a single command.

---

## 🏗️ Architecture

```
PostgreSQL  →  Debezium  →  Kafka  →  MinIO  →  Snowflake  →  dbt  →  Analytics
(Source DB)    (CDC)        (Stream)   (Lake)    (Warehouse)  (Transform)
```

### How data flows:

1. **PostgreSQL** acts as the production banking database — customers, accounts, and transactions
2. **Debezium** reads PostgreSQL's WAL (Write-Ahead Log) and captures every row-level change
3. **Apache Kafka** receives CDC events and streams them with schema enforcement via Confluent Schema Registry
4. A **Python consumer** reads Kafka topics and writes events to **MinIO** as Parquet files
5. **Apache Airflow** DAGs detect new files in MinIO and trigger **Snowflake** loads via `COPY INTO`
6. **dbt** transforms raw data through Medallion Architecture (Bronze → Silver → Gold)

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Source Database | PostgreSQL | Transactional banking data with WAL enabled |
| Change Data Capture | Debezium | Row-level change capture from PostgreSQL WAL |
| Schema Enforcement | Confluent Schema Registry | Validates event structure across all topics |
| Event Streaming | Apache Kafka | High-throughput distributed message bus |
| Data Generator | Python | Simulates real banking transactions continuously |
| Data Lake | MinIO (S3-compatible) | Stores Kafka events as Parquet files |
| Orchestration | Apache Airflow | Schedules and monitors the full pipeline |
| Cloud Warehouse | Snowflake | Analytics-ready cloud data warehouse |
| Transformation | dbt | Medallion Architecture models (Bronze/Silver/Gold) |
| Infrastructure | Docker + Docker Compose | Full stack containerized, one-command setup |

---

## 🚀 Key Features

### ⚡ Real-Time Change Data Capture
- Debezium monitors PostgreSQL WAL logs and captures every `INSERT`, `UPDATE`, `DELETE`
- Zero performance impact on the source database
- Full change history preserved for complete audit trail

### 📨 Event-Driven Streaming with Kafka
- Apache Kafka handles high-throughput event streaming
- Confluent Schema Registry enforces Avro schema on every message
- Topic partitioning by transaction type for parallel processing
- Message retention for fault tolerance and replay capability

### 🗄️ Efficient Data Lake Storage
- Python consumer deserializes Kafka events and writes Parquet files to MinIO
- Files partitioned by date for efficient Snowflake ingestion
- MinIO configured as external stage in Snowflake

### ❄️ Cloud Warehouse Loading
- Airflow sensors detect new Parquet files in MinIO
- Snowflake `COPY INTO` command for bulk efficient loading
- Separate raw schema preserves Bronze layer data as-is

### 🥇 Medallion Architecture with dbt

| Layer | What happens |
|-------|-------------|
| 🥉 Bronze | Raw CDC events loaded exactly as captured from Kafka |
| 🥈 Silver | Deduplication, type casting, CDC op filtering (I/U/D), null handling |
| 🥇 Gold | Account balances, transaction summaries, fraud pattern analytics |

### 🐳 Fully Containerized
- Entire stack runs via single `docker-compose up` command
- Services: PostgreSQL, Debezium, Kafka, Zookeeper, Schema Registry, MinIO, Airflow

---

## 📁 Project Structure

```
modern-banking-data-pipeline/
│
├── airflow/
│   └── dags/                   # Airflow DAGs (MinIO → Snowflake ingestion)
│
├── consumer/                   # Python Kafka consumer → Parquet → MinIO
│
├── data-generator/             # Python script simulating banking transactions
│
├── kafka-debezium/             # Debezium connector configuration
│
├── postgres/                   # PostgreSQL init scripts & schema
│
├── Dockerfile.airflow          # Custom Airflow image
├── docker-compose.yml          # Full stack orchestration
├── my_connector.json           # Debezium Kafka connector config
└── requirements.txt            # Python dependencies
```

---

## ⚙️ How to Run

### Prerequisites
- Docker & Docker Compose installed
- Snowflake account (free trial works)

### 1. Clone the repository
```bash
git clone https://github.com/Sheesaliakbar/modern-banking-data-pipeline.git
cd modern-banking-data-pipeline
```

### 2. Start the full stack
```bash
docker-compose up -d
```

### 3. Register the Debezium connector
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @my_connector.json
```

### 4. Access the tools

| Tool | URL | Credentials |
|------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8081 | — |

### 5. Verify data flow
- Check Kafka topics receiving CDC events
- Verify Parquet files appearing in MinIO
- Trigger Airflow DAG to load into Snowflake
- Run dbt models for transformations

---

## 📊 Business Analytics (Gold Layer)

Once data reaches the Gold layer in Snowflake, the following insights are available:

- **Account Balances** — real-time running balance per account
- **Transaction Velocity** — volume and frequency analysis per customer
- **Fraud Pattern Detection** — anomaly detection based on transaction patterns
- **Customer Summaries** — aggregated customer financial behaviour

---

## 👨‍💻 Author

**Shees Ali Akbar** — Data Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/sheesaliakbar6169/)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat-square&logo=github&logoColor=white)](https://github.com/Sheesaliakbar)
[![Email](https://img.shields.io/badge/Email-EA4335?style=flat-square&logo=gmail&logoColor=white)](mailto:sheesaliakbar@gmail.com)

> Open to Data Engineering opportunities — Remote & On-site (Pakistan)
