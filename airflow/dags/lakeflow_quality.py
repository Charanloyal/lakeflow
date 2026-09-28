"""
### Data quality validation (every 15 minutes)

Runs every contract-derived check (`lakeflow_core.quality.build_checks`, the same SQL the control-plane API runs):
primary-key uniqueness, required fields, enum validity, referential integrity, row-for-row reconciliation of silver
against PostgreSQL, freshness p95 against the contract SLA, bronze event-id uniqueness and open DLQ records.

Results are appended to `lakehouse.ops.quality_results` with `runner = 'airflow'`. A failing **critical** check
(or a check that could not run) fails the task, which is the alerting hook; warnings are recorded only.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from lakeflow_dag_support import CONTRACTS_DIR, DEFAULT_ARGS, START_DATE, TRINO_HOST, TRINO_PORT

from lakeflow_core.contracts import load_registry
from lakeflow_core.quality import RESULTS_DDL, build_checks, results_insert, run_checks


@dag(
    dag_id="lakeflow_quality_checks",
    schedule="*/15 * * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakeflow", "quality"],
    doc_md=__doc__,
)
def lakeflow_quality_checks():
    @task
    def validate() -> dict:
        import trino  # noqa: PLC0415 - keep DAG parsing light

        checks = build_checks(load_registry(CONTRACTS_DIR))
        conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="airflow", catalog="lakehouse")
        try:
            cursor = conn.cursor()
            results = run_checks(cursor, checks)
            run_id, run_at = uuid.uuid4().hex[:12], datetime.now(timezone.utc)
            cursor.execute(RESULTS_DDL)
            cursor.fetchall()
            sql, params = results_insert(run_id, run_at, "airflow", results)
            cursor.execute(sql, params)
            cursor.fetchall()
        finally:
            conn.close()
        for result in results:
            print(json.dumps(result, default=str))
        broken = [r["check_id"] for r in results if r["status"] == "error" or (r["severity"] == "critical" and r["status"] == "fail")]
        if broken:
            raise AirflowException(f"quality run {run_id}: critical checks failed or errored: {broken}")
        return {"run_id": run_id, "checks": len(results), "warnings": [r["check_id"] for r in results if r["status"] == "fail"]}

    validate()


lakeflow_quality_checks()
