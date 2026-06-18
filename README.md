# E-Commerce Real-time Data Pipeline

A near real-time data pipeline that captures changes from an e-commerce transactional database (OLTP) and streams them into an analytical data warehouse (OLAP) for business intelligence dashboards.

## Architecture Overview

```
PostgreSQL (OLTP) → Debezium (CDC) → Kafka → ClickHouse (OLAP) → Metabase (BI)
                                                    ↑
                                              Airflow (Orchestration)
```

## Tech Stack

| Component | Technology | Role |
|-----------|-----------|------|
| OLTP Database | PostgreSQL | Transactional data source |
| CDC | Debezium | Capture data changes via WAL |
| Message Queue | Apache Kafka (KRaft) | Event streaming |
| OLAP Database | ClickHouse | Columnar analytics warehouse |
| Data Modeling | Medallion Architecture | Bronze → Silver → Gold layers |
| Orchestration | Apache Airflow | DAG scheduling & ETL jobs |
| Visualization | Metabase | BI dashboards |
| Infrastructure | Docker Compose | Container orchestration |

## Project Status

🚧 **Work in progress** — Building phase by phase.

- [ ] Docker Compose infrastructure setup
- [ ] PostgreSQL OLTP schema design
- [ ] Airflow DAGs for data generation (Faker)
- [ ] Debezium CDC + Kafka configuration
- [ ] ClickHouse Medallion architecture (Bronze/Silver/Gold)
- [ ] Metabase dashboards
- [ ] End-to-end testing & validation
