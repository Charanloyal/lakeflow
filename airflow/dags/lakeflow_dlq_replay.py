"""
### DLQ replay (manual)

Replays open dead-letter records through the control-plane API (`POST /api/quality/dlq/replay`), which re-publishes
the original bytes to `lakeflow.replay.cdc` with provenance headers and audits the action. Replay is useful after a
contract promotion (for example proposed orders v3 accepting JPY); records that still violate the current contract
return to the DLQ with `replay_attempts` incremented.

Params: optional `error_code` filter, `limit` (max 50 per run, the API bound) and `dry_run` (default true: list only).
"""

from __future__ import annotations

import urllib.parse

from airflow.decorators import dag, task
from airflow.models.param import Param
from lakeflow_dag_support import DEFAULT_ARGS, START_DATE, api_call


@dag(
    dag_id="lakeflow_dlq_replay",
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=["lakeflow", "dlq", "replay"],
    doc_md=__doc__,
    params={
        "error_code": Param(None, type=["null", "string"]),
        "limit": Param(50, type="integer", minimum=1, maximum=50),
        "dry_run": Param(True, type="boolean"),
    },
)
def lakeflow_dlq_replay():
    @task
    def replay_open_records(params: dict | None = None) -> dict:
        params = params or {}
        query = urllib.parse.urlencode({"status": "open", "limit": params.get("limit", 50)})
        records = api_call("GET", f"/api/quality/rejected?{query}")["records"]
        if params.get("error_code"):
            records = [r for r in records if r["error_code"] == params["error_code"]]
        ids = [r["dlq_id"] for r in records]
        print(f"{len(ids)} open DLQ records selected: {ids}")
        if params.get("dry_run", True) or not ids:
            return {"selected": ids, "replayed": [], "dry_run": bool(params.get("dry_run", True))}
        result = api_call("POST", "/api/quality/dlq/replay", {"dlq_ids": ids})
        return {"selected": ids, "replayed": result["replayed"], "missing": result["missing"], "dry_run": False}

    replay_open_records()


lakeflow_dlq_replay()
