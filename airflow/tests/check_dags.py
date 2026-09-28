"""DAG integrity checks, run inside the real Airflow image: `docker compose exec airflow python /opt/airflow/tests/check_dags.py`."""

from __future__ import annotations

import sys

from airflow.models import DagBag

EXPECTED = {
    "lakeflow_table_maintenance": "@hourly",
    "lakeflow_quality_checks": "*/15 * * * *",
    "lakeflow_backfill": None,
    "lakeflow_dlq_replay": None,
}


def main() -> int:
    bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    problems = [f"import error in {path}: {error}" for path, error in bag.import_errors.items()]
    for dag_id, schedule in EXPECTED.items():
        dag = bag.dags.get(dag_id)
        if dag is None:
            problems.append(f"{dag_id} is missing")
            continue
        if dag.schedule_interval != schedule:
            problems.append(f"{dag_id}: schedule {dag.schedule_interval!r} != {schedule!r}")
        if dag.catchup or dag.max_active_runs != 1:
            problems.append(f"{dag_id}: catchup must be off and max_active_runs 1")
        if not dag.tags or "lakeflow" not in dag.tags or not dag.doc_md:
            problems.append(f"{dag_id}: needs the lakeflow tag and documentation")
        for task in dag.tasks:
            if task.owner != "lakeflow" or task.execution_timeout is None:
                problems.append(f"{dag_id}.{task.task_id}: owner/execution_timeout not set")
    unexpected = sorted(set(bag.dag_ids) - set(EXPECTED))
    if unexpected:
        problems.append(f"unexpected DAGs {unexpected}")
    for problem in problems:
        print(f"FAIL {problem}")
    print(f"{len(bag.dags)} DAGs parsed, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
