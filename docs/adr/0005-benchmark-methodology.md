# ADR-0005: Benchmark methodology

**Status**: Accepted
**Date**: 2026-09-25

## Context
v1 published 423k events/s from a Python loop that never touched Kafka, Spark, or Iceberg, and a 75× speedup
that compared two different queries. Any number shown by LakeFlow has to be reproducible from raw evidence and
has to measure the real pipeline.

## Decision
- **The system under test is the running stack.** The runner writes real PostgreSQL transactions. It measures
  what Debezium, Kafka, Spark, and Iceberg deliver, and it reads the results back through Trino. Nothing is
  simulated.
- **Deterministic workload:** a seeded PRNG (`--seed`, default 42, iteration *i* uses seed+i) generates the
  mutation mix (60 % insert, 30 % update, 10 % delete), amounts, and statuses. Each iteration runs under its own
  generated customer, so it can be isolated and reconciled.
- **Metrics (definitions):**
  - *freshness* per event = bronze `committed_at` (Iceberg commit of its batch) − `source_ts` (PostgreSQL commit
    time from Debezium `source.ts_ms`). Reported as p50/p95/p99/mean/stddev over all events of all iterations.
  - *throughput* = events observed ÷ (last commit − first source commit) per iteration. Reported as the
    mean/stddev/min across iterations.
  - *loss rate* = (expected changes − distinct bronze events) ÷ expected. *Duplicate rate* = duplicate bronze
    rows ÷ events. *Error rate* = DLQ records ÷ expected.
  - *reconciliation mismatches* = rows where silver ≠ PostgreSQL after the run.
  - *query benchmark:* the **same** Trino query before and after `optimize`, with separate cold (first run) and
    warm (subsequent runs) samples.
- **Recorded metadata:** git SHA, UTC timestamp, CPU model and count, RAM, OS, Docker version and per-container
  memory/CPU limits, profile, events, seed, batch size, payload bytes, topic partitions, trigger interval,
  `maxOffsetsPerTrigger`, Iceberg write properties, and the CI run URL when available.
- **Raw results** are versioned JSON (`benchmarks/results/*.json`, schema version 1) that contain every latency
  sample. The API and UI **recompute** summaries from those samples (`lakeflow_core.benchmark`). Summaries are
  never stored separately, so a displayed number cannot drift from its evidence.
- **CI regression gate** (`benchmarks/thresholds.json`): loss = 0, duplicates = 0, errors = 0, mismatches = 0,
  p95 freshness ≤ 60 s, throughput ≥ a deliberately low floor. Shared CI runners are noisy, so the gate catches
  breakage, not 10 % regressions.

## Consequences
- Numbers depend on hardware and profile. Every result carries its environment, and results from different
  environments are not compared with each other.
- Local-mode Spark with a 5 s trigger sets a floor on freshness of about one trigger interval plus commit time.
  That is a property of the design, and the results report it rather than hide it.
- Long soak tests (hours) and failure-injection-under-load benchmarks are on the roadmap. They are not claimed.
