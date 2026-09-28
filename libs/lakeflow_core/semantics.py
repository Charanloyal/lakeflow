"""Reference model of how one micro-batch is applied to the lakehouse.

The Spark sink implements the same rules with DataFrames and MERGE. The PySpark tests replay random event
sequences through both and require identical results, so this module is the executable spec in ADR-0003.

Rules, in order:
1. In-batch duplicates (same event_id) keep the first occurrence by Kafka (topic, partition, offset).
2. Events whose event_id is already in bronze from an *earlier* batch are duplicates, and they are skipped
   everywhere. Events found in bronze with the *same* batch id were written by a crashed attempt of this
   batch. They are re-planned but not re-appended.
3. Per primary key, the batch-latest event is the one with the highest (source_lsn, kafka_offset).
4. The baseline is the silver state before this batch. If silver was already updated by this batch
   (crash after commit), `_prev_source_lsn` restores the baseline, so outcomes do not change on replay.
5. The outcome is `stale` if lsn <= baseline lsn, `superseded` if it is newer but not batch-latest, and
   `applied` otherwise. Only applied events change silver. Deletes become tombstones with is_deleted=true.
6. `is_late` is true if source_ts < watermark. The watermark is max(source_ts) - allowed_lateness over
   earlier batches. Late events are never dropped: lateness is only reported, and the LSN guard decides
   what gets applied.
"""

from __future__ import annotations

from dataclasses import dataclass, field

OUTCOME_APPLIED = "applied"
OUTCOME_SUPERSEDED = "superseded"
OUTCOME_STALE = "stale"


@dataclass(frozen=True)
class ChangeEvent:
    event_id: str
    table: str
    pk: str
    op: str
    lsn: int
    ts_ms: int
    topic: str
    partition: int
    offset: int
    image: dict | None = None


@dataclass
class SilverRow:
    pk: str
    values: dict | None
    is_deleted: bool
    source_lsn: int
    prev_source_lsn: int | None
    event_id: str
    batch_id: int


@dataclass
class BatchPlan:
    batch_id: int
    outcomes: dict[str, str] = field(default_factory=dict)
    late: set[str] = field(default_factory=set)
    in_batch_duplicates: int = 0
    prior_duplicates: set[str] = field(default_factory=set)
    bronze_appends: list[str] = field(default_factory=list)
    to_apply: list[ChangeEvent] = field(default_factory=list)
    watermark_after_ms: int = 0


def plan_batch(
    batch_id: int,
    events: list[ChangeEvent],
    silver: dict[tuple[str, str], SilverRow],
    bronze_batches: dict[str, int],
    watermark_ms: int,
    allowed_lateness_ms: int,
) -> BatchPlan:
    plan = BatchPlan(batch_id=batch_id, watermark_after_ms=watermark_ms)
    seen: dict[str, ChangeEvent] = {}
    for event in sorted(events, key=lambda e: (e.topic, e.partition, e.offset)):
        if event.event_id in seen:
            plan.in_batch_duplicates += 1
            continue
        seen[event.event_id] = event

    candidates: list[ChangeEvent] = []
    for event in seen.values():
        existing = bronze_batches.get(event.event_id)
        if existing is not None and existing < batch_id:
            plan.prior_duplicates.add(event.event_id)
            continue
        candidates.append(event)
        if existing is None:
            plan.bronze_appends.append(event.event_id)

    latest: dict[tuple[str, str], ChangeEvent] = {}
    for event in candidates:
        key = (event.table, event.pk)
        best = latest.get(key)
        if best is None or (event.lsn, event.offset) > (best.lsn, best.offset):
            latest[key] = event

    for event in candidates:
        key = (event.table, event.pk)
        row = silver.get(key)
        baseline = None
        if row is not None:
            baseline = row.prev_source_lsn if row.batch_id == batch_id else row.source_lsn
        if event.ts_ms < watermark_ms:
            plan.late.add(event.event_id)
        if baseline is not None and event.lsn <= baseline:
            plan.outcomes[event.event_id] = OUTCOME_STALE
        elif latest[key] is not event:
            plan.outcomes[event.event_id] = OUTCOME_SUPERSEDED
        else:
            plan.outcomes[event.event_id] = OUTCOME_APPLIED
            plan.to_apply.append(event)

    if candidates:
        max_ts = max(e.ts_ms for e in candidates)
        plan.watermark_after_ms = max(watermark_ms, max_ts - allowed_lateness_ms)
    return plan


def apply_plan(plan: BatchPlan, silver: dict[tuple[str, str], SilverRow]) -> None:
    """Mutate `silver` exactly like the guarded MERGE (idempotent: re-applying a plan changes nothing)."""
    for event in plan.to_apply:
        key = (event.table, event.pk)
        row = silver.get(key)
        if row is not None and event.lsn <= row.source_lsn:
            continue
        prev = row.source_lsn if row is not None else None
        if event.op == "d":
            values = row.values if row is not None else event.image
            silver[key] = SilverRow(event.pk, values, True, event.lsn, prev, event.event_id, plan.batch_id)
        else:
            silver[key] = SilverRow(event.pk, event.image, False, event.lsn, prev, event.event_id, plan.batch_id)
