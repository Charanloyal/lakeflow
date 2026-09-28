"""
### Backfill / repair (manual)

Requests a Debezium **incremental snapshot** by inserting a signal row into `lakeflow_ops.debezium_signal`.
Debezium re-reads the selected source rows while streaming continues and emits them as `op = r` events, which go
through the normal contract validation and LSN-guarded MERGE. A backfill therefore repairs rows that are missing or
wrong in the lakehouse, and can never overwrite a newer change.

Params: `contract` (orders | customers), optional `keys` (primary-key UUIDs) and optional `updated_since`
(ISO-8601 with offset). Filters are validated and rendered by `lakeflow_core.backfill`; no raw SQL is accepted.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from lakeflow_dag_support import CONTRACTS_DIR, DEFAULT_ARGS, START_DATE, pg_connect

from lakeflow_core.backfill import SIGNAL_INSERT, snapshot_signal
from lakeflow_core.contracts import load_registry


@dag(
    dag_id="lakeflow_backfill",
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=["lakeflow", "backfill", "cdc"],
    doc_md=__doc__,
    params={
        "contract": Param("orders", type="string", enum=["orders", "customers"]),
        "keys": Param([], type="array", description="primary-key UUIDs (empty = whole table)"),
        "updated_since": Param(None, type=["null", "string"], description="ISO-8601 timestamp with offset"),
    },
)
def lakeflow_backfill():
    @task
    def request_incremental_snapshot(params: dict | None = None) -> str:
        params = params or {}
        since = datetime.fromisoformat(params["updated_since"]) if params.get("updated_since") else None
        signal = snapshot_signal(load_registry(CONTRACTS_DIR), params["contract"], params.get("keys"), since)
        with pg_connect() as conn, conn.cursor() as cursor:
            cursor.execute(SIGNAL_INSERT, signal)
        print(f"signal {signal[0]}: {signal[2]}")
        return signal[0]

    request_incremental_snapshot()


lakeflow_backfill()
