# LakeFlow

A production-oriented reference implementation of a CDC lakehouse: PostgreSQL → Debezium → Kafka → Spark
Structured Streaming → Apache Iceberg on MinIO → Trino. It adds a FastAPI + Next.js control plane, Airflow
maintenance, and Prometheus/Grafana observability.

> **Status: v2 rebuild in progress on branch `lakeflow-v2`.** v1 was a static simulation with fabricated
> benchmarks. The audit that drove this rebuild is in [docs/audit/2026-09-25-repository-audit.md](docs/audit/2026-09-25-repository-audit.md).
> Every milestone is validated by CI against the real stack. Numbers only appear here once CI has produced raw
> evidence for them.

## What already works (validated in CI)

- Versioned data contracts (`contracts/`) with backward-transitive compatibility checks and PII policy
- A single decoder / contract validator shared by Spark, the API, and the tests (`libs/lakeflow_core`)
- An idempotent Spark sink: an LSN-guarded MERGE, soft-delete tombstones, deduplicated bronze, a DLQ with replay, and
  batch records that make checkpoint replays visible
  ([ADR-0003](docs/adr/0003-delivery-semantics.md))

## Design records

| ADR | Decision |
|---|---|
| [0001](docs/adr/0001-kafka-partitioning.md) | Kafka keys, partitions, retention |
| [0002](docs/adr/0002-iceberg-partitioning.md) | Iceberg layout, merge-on-read, file-size controls |
| [0003](docs/adr/0003-delivery-semantics.md) | End-to-end delivery and deduplication semantics |
| [0004](docs/adr/0004-schema-compatibility.md) | Contracts, compatibility, PII |
| [0005](docs/adr/0005-benchmark-methodology.md) | Benchmark methodology |

## Quick start (requires Docker with ≥ 6 GB for the `8gb` profile)

```bash
make bootstrap PROFILE=8gb   # generates .env secrets, pulls/builds images
make up PROFILE=8gb          # waits for health, prints URLs
make demo                    # insert → update → crash Spark → delete, with real identifiers
make down
```

No `make` (e.g. Windows)? Use `python scripts/lakeflowctl.py <command>` instead.
