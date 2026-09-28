"""
### Iceberg table maintenance (hourly)

For every LakeFlow table: **compaction** (`optimize`, which also folds merge-on-read delete files into data files),
**snapshot expiry** and **orphan-file cleanup**, all executed by Trino while the stream keeps writing.

Evidence for each table lands in `lakehouse.ops.maintenance_runs`: data/delete files and bytes before and after,
snapshot counts, and a logical-content check (row count + checksum of the rewrite snapshot equals its parent).
A rewrite that changed content, or any Trino error, fails the task (and Airflow retries it).

Retention is 1h for the local demo; production keeps Trino's 7d floor (docs/runbooks/maintenance.md).
"""

from __future__ import annotations

import json

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from lakeflow_dag_support import CONTRACTS_DIR, DEFAULT_ARGS, START_DATE, TRINO_HOST, TRINO_PORT

from lakeflow_core.contracts import load_registry
from lakeflow_core.maintenance import default_tables, maintain_table, trino_query


def _tables() -> list[str]:
    registry = load_registry(CONTRACTS_DIR)
    return default_tables([registry.current(name).target_table for name in registry.names])


@dag(
    dag_id="lakeflow_table_maintenance",
    schedule="@hourly",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakeflow", "iceberg", "maintenance"],
    doc_md=__doc__,
)
def lakeflow_table_maintenance():
    @task(max_active_tis_per_dag=2)
    def maintain(table: str) -> dict:
        record = maintain_table(trino_query(TRINO_HOST, TRINO_PORT, user="airflow"), table, runner="airflow")
        print(json.dumps(record, default=str, indent=2))
        if record["status"] == "failed":
            raise AirflowException(f"{table}: {record['error']}")
        return json.loads(json.dumps(record, default=str))

    maintain.expand(table=_tables())


lakeflow_table_maintenance()
