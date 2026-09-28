"""Airflow DAG runs and the observability stack, exercised through their real APIs (16gb profile).

Tests skip when a profile is not running, unless CI lists it in LAKEFLOW_REQUIRE_PROFILES (then they fail).
"""

import os

import httpx
import pytest
from support import bronze_events, eventually, insert_order, trino_rows, wait_visible

pytestmark = pytest.mark.e2e

REQUIRED = {p.strip() for p in os.environ.get("LAKEFLOW_REQUIRE_PROFILES", "").split(",") if p.strip()}
AIRFLOW = os.environ.get("LAKEFLOW_AIRFLOW_URL", "http://airflow:8080")
PROMETHEUS = os.environ.get("LAKEFLOW_PROMETHEUS_URL", "http://prometheus:9090")
GRAFANA = os.environ.get("LAKEFLOW_GRAFANA_URL", "http://grafana:3000")
GRAFANA_AUTH = (os.environ.get("GRAFANA_ADMIN_USER", "admin"), os.environ.get("GRAFANA_ADMIN_PASSWORD", ""))
TABLES = {"bronze.cdc_events", "silver.orders", "silver.customers", "ops.dlq_events", "ops.batch_commits"}


def require(url: str, profile: str) -> None:
    try:
        httpx.get(url, timeout=5).raise_for_status()
    except httpx.HTTPError as exc:
        if profile in REQUIRED:
            pytest.fail(f"profile {profile!r} is required but {url} is not healthy: {exc}")
        pytest.skip(f"profile {profile!r} is not running")


@pytest.fixture(scope="module")
def airflow():
    require(f"{AIRFLOW}/health", "maintenance")
    auth = (os.environ.get("AIRFLOW_ADMIN_USER", "admin"), os.environ.get("AIRFLOW_ADMIN_PASSWORD", ""))
    with httpx.Client(base_url=f"{AIRFLOW}/api/v1", auth=auth, timeout=30) as client:
        yield client


def run_dag(client: httpx.Client, dag_id: str, conf: dict | None = None, timeout: float = 900) -> dict:
    eventually(lambda: client.get(f"/dags/{dag_id}").status_code == 200, timeout=240, what=f"{dag_id} parsed")
    created = client.post(f"/dags/{dag_id}/dagRuns", json={"conf": conf or {}})
    assert created.status_code == 200, created.text
    run_id = created.json()["dag_run_id"]
    run = eventually(
        lambda: (
            (run := client.get(f"/dags/{dag_id}/dagRuns/{run_id}").json())["state"] in ("success", "failed") and run
        ),
        timeout=timeout,
        interval=5,
        what=f"{dag_id} run {run_id}",
    )
    if run["state"] != "success":
        pytest.fail(f"{dag_id} run {run_id} {run['state']}:\n{task_logs(client, dag_id, run_id)}")
    return run


def task_logs(client: httpx.Client, dag_id: str, run_id: str) -> str:
    base = f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
    out = []
    for ti in client.get(base).json().get("task_instances", []):
        if ti["state"] == "success":
            continue
        params = {"map_index": ti["map_index"]} if ti.get("map_index", -1) >= 0 else {}
        log = client.get(
            f"{base}/{ti['task_id']}/logs/{max(ti['try_number'], 1)}", params=params, headers={"Accept": "text/plain"}
        )
        out.append(f"--- {ti['task_id']}[{ti.get('map_index')}] {ti['state']}\n{log.text[-2500:]}")
    return "\n".join(out)


def trino_now():
    return trino_rows("SELECT current_timestamp AS now")[0]["now"]


def test_maintenance_dag_compacts_every_table_with_evidence(airflow):
    started = trino_now()
    run = run_dag(airflow, "lakeflow_table_maintenance")
    assert run["state"] == "success", run
    rows = trino_rows(
        "SELECT table_name, status, fingerprint_match FROM lakehouse.ops.maintenance_runs "
        "WHERE runner = 'airflow' AND started_at >= ?",
        [started],
    )
    succeeded = {r["table_name"] for r in rows if r["status"] == "succeeded"}
    assert succeeded == TABLES, rows
    assert not [r for r in rows if r["fingerprint_match"] is False]


def test_quality_dag_records_results(airflow):
    started = trino_now()
    run = run_dag(airflow, "lakeflow_quality_checks")
    assert run["state"] == "success", run
    rows = trino_rows(
        "SELECT check_id, status FROM lakehouse.ops.quality_results WHERE runner = 'airflow' AND run_at >= ?",
        [started],
    )
    assert len({r["check_id"] for r in rows}) >= 10, rows
    assert all(r["status"] != "error" for r in rows), rows


@pytest.mark.xfail(reason="Debezium incremental snapshots are not yet proven in CI (docs/runbooks/maintenance.md)")
def test_backfill_dag_emits_incremental_snapshot_events(airflow):
    order_id = insert_order(status="PAID")["order_id"]
    wait_visible(order_id)
    run = run_dag(airflow, "lakeflow_backfill", {"contract": "orders", "keys": [order_id]})
    assert run["state"] == "success", run
    eventually(lambda: any(e["op"] == "r" for e in bronze_events(order_id)), what="snapshot event in bronze")


def test_dlq_replay_dag_dry_run(airflow):
    run = run_dag(airflow, "lakeflow_dlq_replay", {"dry_run": True})
    assert run["state"] == "success", run


def test_prometheus_scrapes_real_exporters_and_loads_alerts():
    require(f"{PROMETHEUS}/-/ready", "observability")
    wanted = {"spark-stream", "lakeflow-api", "minio"}

    def targets_up():
        active = httpx.get(f"{PROMETHEUS}/api/v1/targets", timeout=10).json()["data"]["activeTargets"]
        health = {t["labels"]["job"]: t["health"] for t in active}
        return all(health.get(job) == "up" for job in wanted) and health

    eventually(targets_up, timeout=180, what="Prometheus targets up")
    groups = httpx.get(f"{PROMETHEUS}/api/v1/rules", timeout=10).json()["data"]["groups"]
    alerts = {rule["name"] for group in groups for rule in group["rules"]}
    assert {"LakeFlowStreamDown", "LakeFlowStreamStalled", "LakeFlowReplicationSlotLagHigh"} <= alerts
    result = httpx.get(f"{PROMETHEUS}/api/v1/query", params={"query": "lakeflow_stream_query_active"}).json()
    assert result["data"]["result"] and result["data"]["result"][0]["value"][1] == "1"


def test_grafana_dashboard_is_provisioned_and_reads_prometheus():
    require(f"{GRAFANA}/api/health", "observability")
    dashboard = httpx.get(f"{GRAFANA}/api/dashboards/uid/lakeflow-pipeline", auth=GRAFANA_AUTH, timeout=10)
    assert dashboard.status_code == 200, dashboard.text
    assert len(dashboard.json()["dashboard"]["panels"]) >= 14
    proxied = httpx.get(
        f"{GRAFANA}/api/datasources/proxy/uid/prometheus/api/v1/query",
        params={"query": "lakeflow_consumer_lag_messages"},
        auth=GRAFANA_AUTH,
        timeout=10,
    )
    assert proxied.status_code == 200 and proxied.json()["data"]["result"], proxied.text
