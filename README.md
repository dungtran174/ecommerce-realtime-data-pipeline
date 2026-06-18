# E-Commerce Real-time Data Pipeline

A near real-time data pipeline that captures changes from an e-commerce transactional database (OLTP) and streams them into an analytical data warehouse (OLAP) for business intelligence dashboards.

## Architecture Overview

```
PostgreSQL (OLTP) → Debezium (CDC) → Kafka → ClickHouse (OLAP) → Metabase (BI)
                                                    ↑
                                              Airflow (Orchestration)
```

## Tech Stack

| Component      | Technology             | Role                          |
| -------------- | ---------------------- | ----------------------------- |
| OLTP Database  | PostgreSQL             | Transactional data source     |
| CDC            | Debezium               | Capture data changes via WAL  |
| Message Queue  | Apache Kafka (KRaft)   | Event streaming               |
| OLAP Database  | ClickHouse             | Columnar analytics warehouse  |
| Data Modeling  | Medallion Architecture | Bronze → Silver → Gold layers |
| Orchestration  | Apache Airflow         | DAG scheduling & ETL jobs     |
| Visualization  | Metabase               | BI dashboards                 |
| Infrastructure | Docker Compose         | Container orchestration       |

## Project Status

🚧 **Work in progress** — Building phase by phase.

- [x] Docker Compose infrastructure setup
- [x] PostgreSQL OLTP schema design
- [x] Airflow DAGs for data generation (Faker)
- [x] Debezium CDC + Kafka configuration
- [ ] ClickHouse Medallion architecture (Bronze/Silver/Gold)
- [ ] Metabase dashboards
- [ ] End-to-end testing & validation

## Getting Started

### Prerequisites

- Docker & Docker Compose installed
- Git

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/dungtran174/ecommerce-realtime-data-pipeline.git
cd ecommerce-realtime-data-pipeline

# 2. Create environment file from template
cp .env.example .env

# 3. Start all services
docker-compose up -d

# 4. Wait for services to be healthy, then register Debezium CDC connector
bash debezium/register-connector.sh
```

### Service UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Debezium UI | http://localhost:8084 | — |
| Kafka UI | http://localhost:8085 | — |
| Metabase | http://localhost:3000 | Setup on first run |
| ClickHouse | http://localhost:8123 | default / (empty) |

