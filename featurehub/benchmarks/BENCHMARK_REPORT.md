# FeatureHub Online Feature Store: Latency & Performance Benchmark

**Evaluation Date**: `2026-09-24T18:44:32Z`  
**Test Sample Size**: `10,000` iterations across 3 entities (`customer`, `merchant`, `device`)  
**Throughput**: **`5,995.65 Queries / Sec (QPS)`**  
**SLA Compliance**: `PASS` (p99 < 10ms SLA target met)

---

## 1. Online Feature Retrieval Latency

| Percentile | Latency (ms) | Target SLA | Status |
| :--- | :---: | :---: | :---: |
| **p50 (Median)** | **`0.1226 ms`** | `< 2.0 ms` |  PASS |
| **p90** | **`0.1821 ms`** | `< 4.0 ms` |  PASS |
| **p95** | **`0.2056 ms`** | `< 5.0 ms` |  PASS |
| **p99** | **`0.3639 ms`** | `< 10.0 ms` |  PASS |
| **p99.9** | **`1.055 ms`** | `< 25.0 ms` |  PASS |
| **Mean** | **`0.1382 ms`** | `< 3.0 ms` |  PASS |
| **Min / Max** | `0.0555 ms` / `3.668 ms` | - | - |

---

## 2. End-to-End Prediction Pipeline Latency
*(Encompasses multi-entity online feature retrieval, vector assembly, and ML scoring)*

| Percentile | Latency (ms) | Target SLA |
| :--- | :---: | :---: |
| **p50 (Median)** | **`0.1992 ms`** | `< 5.0 ms` |
| **p95** | **`0.3588 ms`** | `< 10.0 ms` |
| **p99** | **`0.6854 ms`** | `< 20.0 ms` |
| **Mean** | **`0.2291 ms`** | `< 6.0 ms` |

---

## 3. Architecture Highlights
- **Storage Subsystem**: Redis in-memory key-value data structures with pre-warmed entity pipelines.
- **Join Strategy**: Point-in-time snapshot materialization eliminates runtime table scanning.
- **Inference Acceleration**: Optimized NumPy vector dot product yields sub-0.2ms inference latency.
