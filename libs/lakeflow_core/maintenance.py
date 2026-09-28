"""Iceberg table maintenance through Trino, with before/after evidence (Airflow, `make maintenance` and tests use it).

Compaction runs while the stream keeps writing. Iceberg's optimistic concurrency makes that safe: the Spark sink
retries its idempotent statements on commit/validation conflicts, and a Trino rewrite that loses a race fails and is
retried by the caller (Airflow retries). The logical-content check compares the rewrite snapshot with its own parent,
so rows appended concurrently by the stream do not blur the evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from .tables import BATCH_TABLE, BRONZE_TABLE, CATALOG, DLQ_TABLE, MAINTENANCE_TABLE

Query = Callable[[str, Sequence | None], list[dict]]

TASKS = ("optimize", "expire_snapshots", "remove_orphan_files")
_TABLE = re.compile(r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")
_SIZE = re.compile(r"^\d+(B|kB|MB|GB)$")
_DURATION = re.compile(r"^\d+(s|m|h|d)$")

# Order-insensitive identity of the logical rows; a rewrite must leave it unchanged.
_FINGERPRINTS = {
    BRONZE_TABLE: "event_id",
    DLQ_TABLE: "concat(dlq_id, '|', status, '|', CAST(replay_attempts AS varchar))",
    BATCH_TABLE: "concat(stream_epoch, '|', CAST(batch_id AS varchar), '|', CAST(attempts AS varchar))",
}
_SILVER_FINGERPRINT = "concat(_event_id, '|', CAST(_source_lsn AS varchar), '|', CAST(is_deleted AS varchar))"

MAINTENANCE_DDL = (
    f"CREATE TABLE IF NOT EXISTS {CATALOG}.{MAINTENANCE_TABLE} (run_id varchar, table_name varchar, runner varchar, "
    "status varchar, tasks varchar, started_at timestamp(6) with time zone, finished_at timestamp(6) with time zone, "
    "duration_ms bigint, data_files_before bigint, data_files_after bigint, delete_files_before bigint, "
    "delete_files_after bigint, bytes_before bigint, bytes_after bigint, snapshots_before bigint, "
    "snapshots_after bigint, rewrite_snapshot_id bigint, fingerprint_match boolean, error varchar) "
    "WITH (partitioning = ARRAY['day(started_at)'])"
)
RECORD_COLUMNS = (
    "run_id",
    "table_name",
    "runner",
    "status",
    "tasks",
    "started_at",
    "finished_at",
    "duration_ms",
    "data_files_before",
    "data_files_after",
    "delete_files_before",
    "delete_files_after",
    "bytes_before",
    "bytes_after",
    "snapshots_before",
    "snapshots_after",
    "rewrite_snapshot_id",
    "fingerprint_match",
    "error",
)


@dataclass(frozen=True)
class Policy:
    """Local-demo defaults. Production keeps Trino's 7d retention floors (docs/runbooks/maintenance.md)."""

    file_size_threshold: str = "64MB"
    snapshot_retention: str = "1h"
    orphan_retention: str = "1h"

    def __post_init__(self) -> None:
        if not _SIZE.match(self.file_size_threshold):
            raise ValueError(f"file_size_threshold must look like 64MB, got {self.file_size_threshold!r}")
        for name in ("snapshot_retention", "orphan_retention"):
            if not _DURATION.match(getattr(self, name)):
                raise ValueError(f"{name} must look like 1h or 7d, got {getattr(self, name)!r}")


def default_tables(silver_tables: Sequence[str]) -> list[str]:
    return [BRONZE_TABLE, *silver_tables, DLQ_TABLE, BATCH_TABLE]


def qualified(table: str, suffix: str = "") -> str:
    if not _TABLE.match(table):
        raise ValueError(f"invalid table name {table!r} (expected schema.table)")
    schema, name = table.split(".")
    return f'{CATALOG}.{schema}."{name}{suffix}"'


def fingerprint_expr(table: str) -> str:
    qualified(table)
    return _FINGERPRINTS.get(table, _SILVER_FINGERPRINT if table.startswith("silver.") else "1")


def statements(table: str, policy: Policy) -> dict[str, str]:
    target = qualified(table)
    return {
        "optimize": f"ALTER TABLE {target} EXECUTE optimize(file_size_threshold => '{policy.file_size_threshold}')",
        "expire_snapshots": (
            f"ALTER TABLE {target} EXECUTE expire_snapshots(retention_threshold => '{policy.snapshot_retention}')"
        ),
        "remove_orphan_files": (
            f"ALTER TABLE {target} EXECUTE remove_orphan_files(retention_threshold => '{policy.orphan_retention}')"
        ),
    }


def table_exists(query: Query, table: str) -> bool:
    qualified(table)
    schema, name = table.split(".")
    rows = query(
        f"SELECT count(*) AS n FROM {CATALOG}.information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, name],
    )
    return bool(rows and rows[0]["n"])


def file_stats(query: Query, table: str) -> dict:
    rows = query(
        "SELECT count_if(content = 0) AS data_files, count_if(content <> 0) AS delete_files, "  # noqa: S608
        f"coalesce(sum(file_size_in_bytes), 0) AS total_bytes FROM {qualified(table, '$files')}",
        None,
    )
    snaps = query(
        f"SELECT count(*) AS snapshots, max(committed_at) AS last_commit FROM {qualified(table, '$snapshots')}",  # noqa: S608
        None,
    )
    return {**rows[0], **snaps[0]}


def fingerprint(query: Query, table: str, snapshot_id: int) -> tuple[int, str]:
    rows = query(
        f"SELECT count(*) AS row_count, checksum({fingerprint_expr(table)}) AS digest "  # noqa: S608
        f"FROM {qualified(table)} FOR VERSION AS OF {int(snapshot_id)}",
        None,
    )
    digest = rows[0]["digest"]
    return int(rows[0]["row_count"]), digest.hex() if isinstance(digest, (bytes, bytearray)) else str(digest)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def maintain_table(
    query: Query,
    table: str,
    policy: Policy | None = None,
    tasks: Sequence[str] = TASKS,
    runner: str = "cli",
    run_id: str | None = None,
    now: Callable[[], datetime] = _now,
    persist: bool = True,
) -> dict:
    """Run the maintenance tasks for one table and return (and persist) the evidence record."""
    policy = policy or Policy()
    unknown = sorted(set(tasks) - set(TASKS))
    if unknown:
        raise ValueError(f"unknown maintenance tasks {unknown}")
    sql = statements(table, policy)
    started, clock = now(), time.perf_counter()
    record: dict = {column: None for column in RECORD_COLUMNS}
    record.update(run_id=run_id or uuid.uuid4().hex[:12], table_name=table, runner=runner, tasks=",".join(tasks))
    record["started_at"] = started
    try:
        if not table_exists(query, table):
            record.update(status="skipped", error="table does not exist yet")
        else:
            before = file_stats(query, table)
            record.update(
                data_files_before=before["data_files"],
                delete_files_before=before["delete_files"],
                bytes_before=before["total_bytes"],
                snapshots_before=before["snapshots"],
            )
            if "optimize" in tasks:
                query(sql["optimize"], None)
                if before["last_commit"] is not None:
                    rewrite = query(
                        f"SELECT snapshot_id, parent_id FROM {qualified(table, '$snapshots')} "  # noqa: S608
                        "WHERE operation = 'replace' AND committed_at > ? ORDER BY committed_at DESC LIMIT 1",
                        [before["last_commit"]],
                    )
                    if rewrite and rewrite[0]["parent_id"] is not None:
                        snapshot, parent = rewrite[0]["snapshot_id"], rewrite[0]["parent_id"]
                        record["rewrite_snapshot_id"] = snapshot
                        record["fingerprint_match"] = fingerprint(query, table, snapshot) == fingerprint(
                            query, table, parent
                        )
            for task in ("expire_snapshots", "remove_orphan_files"):
                if task in tasks:
                    query(sql[task], None)
            after = file_stats(query, table)
            record.update(
                data_files_after=after["data_files"],
                delete_files_after=after["delete_files"],
                bytes_after=after["total_bytes"],
                snapshots_after=after["snapshots"],
            )
            if record["fingerprint_match"] is False:
                record.update(status="failed", error="rewrite changed the logical content of the table")
            else:
                record["status"] = "succeeded"
    except Exception as exc:  # noqa: BLE001 - recorded as evidence; callers decide whether to retry
        record.update(status="failed", error=f"{type(exc).__name__}: {str(exc)[:500]}")
    record["finished_at"] = now()
    record["duration_ms"] = int((time.perf_counter() - clock) * 1000)
    if persist:
        try:
            query(MAINTENANCE_DDL, None)
            query(
                f"INSERT INTO {CATALOG}.{MAINTENANCE_TABLE} VALUES ({', '.join('?' * len(RECORD_COLUMNS))})",  # noqa: S608
                [record[c] for c in RECORD_COLUMNS],
            )
        except Exception as exc:  # noqa: BLE001 - the returned record still carries the evidence
            record["persist_error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    return record


def trino_query(host: str, port: int, user: str = "lakeflow-maintenance") -> Query:
    import trino  # noqa: PLC0415 - optional dependency; the core library stays stdlib-only

    def query(sql: str, params: Sequence | None = None) -> list[dict]:
        conn = trino.dbapi.connect(host=host, port=port, user=user, catalog=CATALOG)
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, list(params))
            else:
                cursor.execute(sql)
            rows = cursor.fetchall()
            names = [d[0] for d in cursor.description or []]
            return [dict(zip(names, row)) for row in rows] if names else []
        finally:
            conn.close()

    return query


def main(argv: Sequence[str] | None = None) -> int:
    from .contracts import load_registry  # noqa: PLC0415

    parser = argparse.ArgumentParser(description="Compact, expire and clean LakeFlow Iceberg tables via Trino")
    parser.add_argument("--tables", nargs="*", help="schema.table names (default: every LakeFlow table)")
    parser.add_argument("--tasks", nargs="*", default=list(TASKS), choices=TASKS)
    parser.add_argument("--runner", default="cli")
    parser.add_argument("--contracts", default=os.environ.get("LAKEFLOW_CONTRACTS_DIR", "contracts"))
    args = parser.parse_args(argv)
    registry = load_registry(args.contracts)
    tables = args.tables or default_tables([registry.current(n).target_table for n in registry.names])
    query = trino_query(os.environ.get("LAKEFLOW_TRINO_HOST", "trino"), int(os.environ.get("LAKEFLOW_TRINO_PORT", "8080")))
    run_id, failed = uuid.uuid4().hex[:12], 0
    for table in tables:
        record = maintain_table(query, table, tasks=args.tasks, runner=args.runner, run_id=run_id)
        failed += record["status"] == "failed"
        print(json.dumps(record, default=str))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
