# Prerequisites & System Requirements

Before running the **E-Commerce Real-time Data Pipeline**, verify that your host environment meets the hardware and software prerequisites below.

---

## 1. Hardware Requirements

Running the full containerized stack (13 services including Kafka, ClickHouse, Airflow, PostgreSQL instances, Debezium, and Metabase) requires adequate compute resources:

| Resource | Minimum | Recommended |
| :--- | :--- | :--- |
| **CPU** | 4 Cores | 8 Cores |
| **RAM** | 8 GB | 16 GB |
| **Storage** | 20 GB free disk space | 50 GB free (SSD preferred) |
| **OS** | Linux (Ubuntu 20.04+), macOS, or Windows (WSL 2) | Ubuntu 22.04 LTS |

> [!WARNING]
> If using **Docker Desktop** (macOS or Windows WSL 2), ensure that at least **8 GB of RAM** and **4 CPUs** are allocated in `Settings -> Resources`. Otherwise, memory-intensive services like ClickHouse and Kafka KRaft may encounter Out-Of-Memory (OOM) errors during startup.

---

## 2. Software Requirements

Ensure the following tools are installed:

### Docker & Docker Compose
- **Docker Engine**: Version `24.0.0` or higher
- **Docker Compose**: Version `v2.20.0` or higher

```bash
docker --version
docker compose version
```

### Git
- Version `2.30.0` or higher:
```bash
git --version
```

### Client CLI Utilities (Optional for manual inspection)
- **curl** & **jq**: For inspecting Debezium REST API
- **psql**: PostgreSQL client (`sudo apt install postgresql-client`)

---

## 3. Host Port Allocation

The following host ports must be free:

| Port | Service | Role |
| :--- | :--- | :--- |
| `5432` | `postgres-main` | E-commerce OLTP transactional database |
| `5433` | `postgres-airflow` | Airflow metadata database |
| `5434` | `postgres-metabase` | Metabase application database |
| `8080` | `airflow-apiserver` | Airflow Web UI & REST API |
| `8083` | `debezium` | Debezium Connect REST API |
| `8084` | `debezium-ui` | Debezium Web Management UI |
| `8085` | `kafka-ui` | Kafka Web Management UI |
| `9092` | `kafka` | Apache Kafka plaintext listener |
| `8123` | `clickhouse` | ClickHouse HTTP API |
| `9000` | `clickhouse` | ClickHouse native TCP port |
| `3000` | `metabase` | Metabase Business Intelligence UI |

Check for existing port bindings on Linux:
```bash
sudo lsof -i :5432 -i :8080 -i :8083 -i :8084 -i :8085 -i :9092 -i :8123 -i :3000
```
