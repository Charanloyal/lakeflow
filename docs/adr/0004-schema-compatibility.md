# ADR-0004: Data contracts, schema compatibility and PII

**Status**: Accepted
**Date**: 2026-09-25

## Context
Source schemas change: columns get added and enums get widened. Consumers need to know which changes are safe,
what happens to records that break the contract, and how to recover them. The customer table carries direct PII
that must not reach the lakehouse in clear text.

## Decision
- **Contracts are JSON Schema (draft 2020-12)** in `contracts/<name>/v<N>.schema.json`, limited to a validated
  subset: type, enum, pattern, format, lengths, `x-decimal`, and `x-pii`. Unsupported keywords fail at load time,
  so no rule is silently ignored. `x-lakeflow` carries the owner, source table and topic, primary key,
  classification, freshness SLA, target table, and references.
- **Compatibility mode `BACKWARD_TRANSITIVE`:** each version must accept every record valid under *all* earlier
  versions. `check_backward_compatible` enforces this at load time and in CI. Allowed: new optional fields,
  enum widening, relaxed lengths, type widening (integer → number). Forbidden: new required fields, narrowed
  enums or types, changed patterns or formats, primary-key changes.
- **Version stamping:** each event records the *earliest* active version that validates it
  (`contract_version`). This shows which producers still emit v1-shaped rows.
- **Drift:** fields unknown to every active version are tolerated (the change is additive) and reported in
  `drift_fields`. They are **dropped** from stored images, because unknown fields have no PII classification.
- **Violations** go to `ops.dlq_events` with reason codes (`amount.pattern`, `status.enum`, …). The stream
  never stops for bad data.
- **Recovery workflow:** propose `contracts/<name>/proposed/v<N+1>` → CI compatibility check →
  `lakeflowctl contract promote` → the Spark job hot-reloads contracts at the next micro-batch and evolves the
  Iceberg schema (`ADD COLUMN`) → replay the DLQ records through `lakeflow.replay.cdc`. The runbook scenario is
  JPY: the source accepts it, v2 rejects it, and proposed v3 widens the enum.
- **PII:** `x-pii: direct` fields must be `hash` (HMAC-SHA256 with `LAKEFLOW_PII_HMAC_KEY`, so values stay
  joinable without being reversible) or `drop`. Customer email → `email_hmac`. `full_name` is dropped before
  bronze. Reconciliation SQL never compares PII columns.
- **Referential integrity** (orders → customers) is checked after the fact with a 300 s grace period. It is not
  enforced at ingestion, because ordering across topics is not guaranteed (ADR-0001).

## Consequences
- A single pure-Python validator serves the Spark job (as a UDF), the API, and the tests, so they cannot
  disagree. The cost is Python UDF serialization, which the benchmark measures. A native-expression compiler is
  on the roadmap.
- Contract changes are code-reviewed files, not runtime toggles. Hot reload only picks up changes that pass the
  compatibility checks. An incompatible file is rejected, and the previous registry stays active (a metric
  records the rejection).
