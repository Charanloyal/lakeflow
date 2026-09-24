# LakeFlow | Streaming CDC Lakehouse Platform (`data-platform-lab`)

[![GitHub Pages](https://img.shields.io/badge/Live%20Demo-GitHub%20Pages-10b981?style=for-the-badge&logo=github&logoColor=white)](https://charanloyal.github.io/lakeflow/)
[![Render Demo](https://img.shields.io/badge/Cloud%20Demo-Render-00c7b7?style=for-the-badge&logo=render&logoColor=white)](https://lakeflow-demo.onrender.com)
[![Local Dashboard](https://img.shields.io/badge/Local%20Dashboard-8501-f59e0b?style=for-the-badge&logo=streamlit&logoColor=black)](http://localhost:8501)
[![API Docs](https://img.shields.io/badge/REST%20API-Swagger%20Docs-3b82f6?style=for-the-badge&logo=fastapi&logoColor=white)](http://localhost:8000/docs)
[![Throughput](https://img.shields.io/badge/Throughput-423%2C062%20evt%2Fs-10b981?style=for-the-badge)](file:///lakeflow/benchmarks/report.md)
[![Query Speedup](https://img.shields.io/badge/Query%20Speedup-75.4x%20Faster-d4af37?style=for-the-badge)](file:///lakeflow/benchmarks/report.md)

A production-grade, local development and cloud-ready infrastructure monorepo powering modern data engineering architectures:
* **LakeFlow**: Streaming Change Data Capture (CDC) Lakehouse Platform
* **FeatureHub**: Real-Time Feature Store (Online/Offline)
* **DataGuard**: Data Quality, Contracts, and Lineage Platform

---

## 🌐 Live Demo & Service Access Directory

| Environment | Service / App | Access Link | Description | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Instant Live Demo** | **GitHub Pages Live App** | **[`https://charanloyal.github.io/lakeflow/`](https://charanloyal.github.io/lakeflow/)** | 24/7 free hosted interactive web UI (zero sleep time) | 🟢 **ACTIVE** |
| **Cloud Container** | **Render Cloud Service** | **[`https://lakeflow-demo.onrender.com`](https://lakeflow-demo.onrender.com)** | Hosted on Render via `render.yaml` / Dockerfile | 🟡 **Needs 1-click connect** |
| **Local Web App** | **LakeFlow 2.0 Web UI** | **[`http://localhost:8501`](http://localhost:8501)** | Modern Black & Gold Glassmorphic UI with pipeline canvas | 🟢 **ACTIVE** |
| **REST API** | **FastAPI Swagger Docs** | **[`http://localhost:8000/docs`](http://localhost:8000/docs)** | OpenAPI interactive testing for CDC mutation endpoints | 🟢 **ACTIVE** |
| **Compute** | **Spark Master UI** | `http://localhost:8080` | Real-time micro-batch streaming listener status | 🟢 Local |
| **SQL Engine** | **Trino Query UI** | `http://localhost:8082` | Distributed execution plans and partition pruning stats | 🟢 Local |
| **Object Store** | **MinIO S3 Console** | `http://localhost:9001` | Parquet files and Iceberg metadata manifests | 🟢 Local |

> 💡 **Why Render says "Not Found":**  
> Render requires you to connect your repository once in your Render dashboard:
> 1. Go to **[dashboard.render.com/blueprints/new](https://dashboard.render.com/blueprints/new)** (or **New + ➔ Web Service**)
> 2. Select `Charanloyal/lakeflow`
> 3. Click **Apply / Create Web Service** — Render will build `Dockerfile` and publish to `https://lakeflow-demo.onrender.com`!

---

## 1. Architecture Overview

```
                                    +-------------------------------------------------------+
                                    |                 data-platform-network                 |
                                    +-------------------------------------------------------+
                                                                |
        +-----------------------------------+                   |                   +----------------------------------+
        |   Streaming & CDC Ingestion       |                   |                   |   Storage & Lakehouse Layer      |
        +-----------------------------------+                   |                   +----------------------------------+
        |  PostgreSQL 16 (Logical WAL)      |                   |                   |  MinIO (S3 Object Store)         |
        |      | (CDC Events)               |                   |                   |    - s3a://warehouse/            |
        |  Debezium Connect (Port 8083)     |                   |                   |    - s3a://lakeflow/             |
        |      | (Kafka Topics)             |                   |                   |  Apache Iceberg Format           |
        |  Apache Kafka 3.7 (KRaft: 9092)   |                   |                   +----------------------------------+
        +-----------------+-----------------+                   |                                    ^
                          |                                     |                                    |
                          v                                     |                                    |
        +-----------------------------------+                   |                   +----------------+-----------------+
        |   Compute & Stream Processing     |-------------------+------------------>|   Interactive SQL Analytics      |
        +-----------------------------------+                   |                   +----------------------------------+
        |  Apache Spark 3.5.1               |                   |                   |  Trino 446 (Distributed SQL)     |
        |    - Spark Master (7077 / 8080)   |                   |                   |    - Iceberg Catalog (Port 8082) |
        |    - Spark Worker (Port 8081)     |                   |                   |    - PostgreSQL Catalog          |
        +-----------------------------------+                   |                   +----------------------------------+
                                                                |
        +-----------------------------------+                   |                   +----------------------------------+
        |   Orchestration & Feature Serving |                   |                   |   Observability & Telemetry      |
        +-----------------------------------+                   |                   +----------------------------------+
        |  Redis 7.2 (Feature Store: 6379)  |                   |                   |  Prometheus 2.52 (Scrapes: 9090) |
        |  Apache Airflow 2.9 (Web: 8888)   |                   |                   |  Grafana 10.4 (Dashboards: 3000) |
        +-----------------------------------+                   |                   +----------------------------------+
```

---

## 2. How the Infrastructure Works End-to-End

### Pillar 1: CDC & Streaming Ingestion (LakeFlow Foundation)
1. **Source Mutations**: Writes occur in `PostgreSQL` (`platform_db.platform.telemetry_events`). With `wal_level=logical` and a 5s automated heartbeat, PostgreSQL streams changes into WAL segments without disk bloat.
2. **Change Capture**: **Debezium Connect** reads the logical replication stream without polling or impacting read query load.
3. **Event Log**: Debezium produces structured JSON/Avro events to **Kafka** topics with partition keys matching primary keys.

### Pillar 2: Lakehouse Storage & Processing
1. **Stream / Batch Ingestion**: **Apache Spark** Structured Streaming reads from Kafka topics with exactly-once semantics.
2. **Open Table Format**: Spark writes ACID Parquet snapshots into **Apache Iceberg** tables stored on **MinIO** S3 (`s3a://warehouse/`).
3. **Compaction & Snapshots**: Iceberg metadata tracks snapshots, partitions, and file compaction for low analytical query latency.

### Pillar 3: Fast Interactive SQL & Online Feature Store (FeatureHub Foundation)
1. **Interactive Query Engine**: **Trino** executes sub-second ANSI SQL queries directly against the Iceberg tables in MinIO or federates queries with PostgreSQL.
2. **Low-Latency Feature Serving**: **Redis** serves sub-12ms p99 online feature lookups for real-time model inference.
3. **Workflow Orchestration**: **Apache Airflow** triggers scheduled transformations, backfills, and catalog compaction jobs.

### Pillar 4: Telemetry & Observability
1. **Metrics Ingestion**: **Prometheus** polls metric endpoints across Trino, MinIO, Redis, and infrastructure targets every 15s.
2. **Unified Dashboards**: **Grafana** provides pre-configured dashboards visualizing cluster resource usage and pipeline latency.

---

## 3. Port & Service Directory

| Service | Host Port | Protocol / URL | Credentials (Default) | Health Check Endpoint |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL** | `5432` | `postgresql://localhost:5432/platform_db` | `postgres` / `postgres` | `pg_isready -U postgres` |
| **Kafka (KRaft)** | `9092` | `localhost:9092` | N/A | `nc -z localhost 9092` |
| **Kafka Connect** | `8083` | `http://localhost:8083` | N/A | `http://localhost:8083/connectors` |
| **Spark Master UI** | `8080` | `http://localhost:8080` | N/A | `http://localhost:8080` |
| **Spark Master RPC** | `7077` | `spark://localhost:7077` | N/A | RPC socket |
| **Spark Worker UI** | `8081` | `http://localhost:8081` | N/A | `http://localhost:8081` |
| **MinIO S3 API** | `9000` | `http://localhost:9000` | `admin` / `password123` | `http://localhost:9000/minio/health/live` |
| **MinIO Web Console**| `9001` | `http://localhost:9001` | `admin` / `password123` | Web UI |
| **Trino Query Engine**| `8082` | `http://localhost:8082` | user: `admin` | `http://localhost:8082/v1/info` |
| **Redis** | `6379` | `localhost:6379` | No password (dev) | `redis-cli ping` |
| **Airflow Web UI** | `8888` | `http://localhost:8888` | `admin` / `admin` | `http://localhost:8888/health` |
| **Prometheus** | `9090` | `http://localhost:9090` | N/A | `http://localhost:9090/-/healthy` |
| **Grafana** | `3000` | `http://localhost:3000` | `admin` / `admin` | `http://localhost:3000/api/health` |

---

## 4. Quickstart Guide

### Option 1: 1-Click Launch Interactive Web App (Instant)
Zero Docker required — runs instantly:
```bash
python run_app.py
```
Open **`http://localhost:8501`** to interact with the full LakeFlow Black & Gold Glassmorphic canvas!

### Option 2: CLI Downstream Mutation Demo
```bash
python scripts/demo.py
```

### Option 3: Full Distributed Docker Infrastructure
```bash
# Start all 12 containers in background
docker compose up -d

# Verify automated health probes across all services
python scripts/verify_health.py

# Stop stack safely
docker compose down
```

---

## 5. LakeFlow Production-Readiness & Benchmark Results

### Measured 2,000,000 CDC Event Benchmark
*(Generated and measured via `apps/benchmarks/benchmark_engine.py`; raw data in `lakeflow/benchmarks/results.json`)*

| Metric | Measured Value | Production SLA | Status |
| :--- | :--- | :--- | :--- |
| **Total Events Ingested** | **2,010,000** | $\ge 2,000,000$ | **PASS** |
| **Total Events Consumed** | **2,000,000** | $\ge 2,000,000$ | **PASS** |
| **Sustained Throughput** | **423,062 events/sec** | $> 25,000$ events/sec | **PASS (16.9x target)** |
| **Micro-batch Latency (p50)** | **223.2 ms** | $< 500$ ms | **PASS** |
| **Micro-batch Latency (p95)** | **235.2 ms** | $< 1,500$ ms | **PASS** |
| **End-to-End Freshness Lag** | **253.9 ms** | $< 3,000$ ms | **PASS** |
| **Duplicate Events Injected** | **10,000** (0.5% stress) | Resilience Test | -- |
| **Duplicate Events Filtered** | **10,000** (100.0% accuracy) | $100\%$ Filtered | **PASS** |
| **Failed / Dropped Events** | **0** | **0** | **PASS** |

### Query Strategy Benchmark Comparison (Trino / Iceberg)

| Strategy | Query Pattern | Measured Latency | Speedup |
| :--- | :--- | :--- | :--- |
| **Strategy A** | Raw CDC full-scan with window deduplication (`ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY lsn DESC)`) | **254.5 ms** | Baseline |
| **Strategy B** | Compacted Iceberg columnar scan with partition pruning (`WHERE order_date = '2026-09-23'`) | **3.4 ms** | **75.4x faster** (98.7% latency reduction) |

### Senior Data Platform Hardening Implemented
1. **CDC Heartbeat Mechanism**: Configured `heartbeat.interval.ms = 5000` to advance replication slot LSNs during idle periods, preventing PostgreSQL WAL segment disk exhaustion.
2. **Deterministic Deduplication**: Composite `(record_key, source_lsn)` watermarked deduplication ensures exactly-once semantics across Kafka consumer rebalances.
3. **RocksDB State Store Provider**: Replaced in-memory streaming state with disk-backed RocksDB to prevent JVM OOMs under high-cardinality streaming state.
4. **Iceberg Small File Prevention**: Set `write.target-file-size-bytes = 134217728` (128MB) with Zstandard Parquet compression to eliminate compaction debt.
