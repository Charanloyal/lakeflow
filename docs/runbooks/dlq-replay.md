# Runbook: dead-letter queue triage and replay

Alert: `LakeFlowDlqReceivingRecords`. Quality check: `dlq.open_records`.

## What lands in `lakehouse.ops.dlq_events`

The decoder never raises. It rejects a record with a reason code: `MALFORMED_JSON`, `MALFORMED_ENCODING`,
`INVALID_ENVELOPE`, `UNSUPPORTED_OP`, `UNKNOWN_TOPIC`, `MISSING_ROW_IMAGE`, `KEY_MISMATCH`, `CONTRACT_VIOLATION`,
`MALFORMED_ROW_IMAGE` or `DECODER_ERROR`. Each DLQ row keeps:

- the exact raw key and value (base64 when not UTF-8),
- the Kafka coordinates (`dlq_id` = sha256 of topic/partition/offset, so redelivery cannot create duplicates),
- the violated contract rules and any `lakeflow-injection-id` header.

## Privacy: the DLQ is a restricted quarantine

A rejected record never passed contract validation, so its raw bytes still hold every field as the source sent
it. That includes direct PII (`customers.email`, `full_name`) that bronze and silver hash or drop. Replay needs
those exact bytes, so `ops.dlq_events` keeps them unchanged. The rules are:

- **Display is masked.** `GET /api/quality/rejected` (and the UI, and therefore the recorded demo) passes every
  payload through `lakeflow_core.redaction`:
  - Contract direct-PII fields and fields unknown to the contract become `[redacted: …]`.
  - Payloads that cannot be masked field by field are withheld when their contract has direct PII. That covers
    unparseable or binary payloads and anything that is not a Debezium envelope.
  - Orders payloads, which have no direct PII, are shown byte-exact.
  - `raw_value_redaction` says which of these happened (`none`, `masked` or `withheld`).
- **Replay is not affected.** It reads the stored bytes server-side and never uses the display copy.
- **Direct table access is privileged.** Anyone with Trino access to `lakehouse.ops.dlq_events` can read the raw
  bytes. The local demo has no table-level access control. A real deployment needs it, plus a retention rule
  (for example, purge `replayed` rows after N days and alert on old `open` rows). **Neither is implemented
  here.**

## Triage

1. Open **Data Quality** and select the *Rejected records* tab, or query:
   `SELECT error_code, count(*) FROM lakehouse.ops.dlq_events WHERE status = 'open' GROUP BY 1`
2. Classify each group:
   - **Producer bug** (malformed JSON, missing row image): fix the producer. Replaying cannot succeed.
   - **Contract drift** (`CONTRACT_VIOLATION`, for example currency `JPY`): decide whether the contract should
     accept the value. If it should, add a backward-compatible version under `contracts/<name>/proposed/`. The CI
     compatibility check (ADR-0004) must pass. Then run `python scripts/lakeflowctl.py contract promote orders 3`.
     The stream hot-reloads contracts at the next micro-batch.
   - **Injected test records** (non-null `injection_id`): expected. Leave them, or resolve them by replay once the
     scenario is over.

## Replay

- UI: open *Rejected records* and choose **Replay** on an open record (admin role).
- Airflow: trigger `lakeflow_dlq_replay` with `{"error_code": "CONTRACT_VIOLATION", "dry_run": false}`. The default
  `dry_run: true` only lists what would be replayed.
- API: `POST /api/quality/dlq/replay {"dlq_ids": [...]}` (at most 50 ids per call).

A replay re-publishes the original bytes to `lakeflow.replay.cdc`. The provenance headers are
`lakeflow-replay-of = <dlq_id>` and `lakeflow-original-topic`. The stream decodes the bytes again with the
current contracts:

- **Success:** the row is applied through the normal LSN-guarded MERGE. It cannot overwrite a newer change. The DLQ
  row becomes `status = 'replayed'` with `resolved_batch_id`.
- **Failure:** the record stays `open`, and `replay_attempts` increments.

## Verify

- `orders.source_reconciliation` passes (after its 120 s in-flight grace period).
- `bronze.cdc_events` holds the replayed event with `replay_of = <dlq_id>`.
