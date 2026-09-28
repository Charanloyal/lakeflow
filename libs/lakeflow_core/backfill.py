"""Backfill = a Debezium incremental snapshot, requested by inserting a signal row into the source database.

Debezium re-reads the selected rows in chunks while streaming continues (watermark-based dedup), and emits them as
`op = r` change events. They flow through the normal decode -> LSN-guarded MERGE path, so a backfill can repair a
lakehouse row that was lost or corrupted but can never overwrite a newer change. Filters are structured and
validated here; users never pass raw SQL to the connector.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime

from .contracts import ContractRegistry

SIGNAL_TABLE = "lakeflow_ops.debezium_signal"
SIGNAL_INSERT = f"INSERT INTO {SIGNAL_TABLE} (id, type, data) VALUES (%s, %s, %s)"  # noqa: S608 - constant name
MAX_KEYS = 500
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def snapshot_signal(
    registry: ContractRegistry,
    contract_name: str,
    keys: list[str] | None = None,
    updated_since: datetime | None = None,
    signal_id: str | None = None,
) -> tuple[str, str, str]:
    """Return (id, type, data) for an `execute-snapshot` signal row."""
    if contract_name not in registry.names:
        raise ValueError(f"unknown contract {contract_name!r}; expected one of {registry.names}")
    contract = registry.current(contract_name)
    fields = registry.all_fields(contract_name)
    table = contract.source_table
    conditions = []
    if keys:
        if len(keys) > MAX_KEYS:
            raise ValueError(f"at most {MAX_KEYS} keys per backfill signal")
        if len(contract.primary_key) != 1 or fields[contract.primary_key[0]].fmt != "uuid":
            raise ValueError("key filters are supported for single-column uuid primary keys")
        normalized = [str(k).strip().lower() for k in keys]
        invalid = [k for k in normalized if not _UUID.match(k)]
        if invalid:
            raise ValueError(f"not UUIDs: {invalid[:5]}")
        conditions.append(f"{contract.primary_key[0]} IN ({', '.join(repr(k) for k in normalized)})")
    if updated_since is not None:
        if "updated_at" not in fields:
            raise ValueError(f"{contract_name} has no updated_at column")
        if updated_since.tzinfo is None:
            raise ValueError("updated_since must be timezone-aware")
        conditions.append(f"updated_at >= '{updated_since.isoformat()}'")
    data: dict = {"type": "incremental", "data-collections": [table]}
    if conditions:
        data["additional-conditions"] = [{"data-collection": table, "filter": " AND ".join(conditions)}]
    return signal_id or f"backfill-{uuid.uuid4().hex[:16]}", "execute-snapshot", json.dumps(data, separators=(",", ":"))


def main(argv: list[str] | None = None) -> int:
    import argparse  # noqa: PLC0415 - CLI only
    import os  # noqa: PLC0415

    import psycopg  # noqa: PLC0415 - optional dependency (tools image)

    from .contracts import load_registry  # noqa: PLC0415

    parser = argparse.ArgumentParser(description="Request a Debezium incremental snapshot (backfill)")
    parser.add_argument("contract")
    parser.add_argument("--keys", nargs="*", default=[])
    parser.add_argument("--updated-since")
    parser.add_argument("--contracts", default=os.environ.get("LAKEFLOW_CONTRACTS_DIR", "contracts"))
    args = parser.parse_args(argv)
    since = datetime.fromisoformat(args.updated_since) if args.updated_since else None
    signal = snapshot_signal(load_registry(args.contracts), args.contract, args.keys, since)
    with psycopg.connect(os.environ["LAKEFLOW_PG_DSN"]) as conn:
        conn.execute(SIGNAL_INSERT, signal)
    print(json.dumps({"signal_id": signal[0], "data": json.loads(signal[2])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
