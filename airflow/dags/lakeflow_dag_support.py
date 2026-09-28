"""Shared settings and clients for LakeFlow DAGs (skipped by the DAG parser through .airflowignore)."""

from __future__ import annotations

import base64
import json
import os
import urllib.request
from datetime import timedelta

import pendulum

CONTRACTS_DIR = os.environ.get("LAKEFLOW_CONTRACTS_DIR", "/opt/airflow/contracts")
TRINO_HOST = os.environ.get("LAKEFLOW_TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("LAKEFLOW_TRINO_PORT", "8080"))
START_DATE = pendulum.datetime(2026, 1, 1, tz="UTC")
DEFAULT_ARGS = {
    "owner": "lakeflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=20),
}


def api_call(method: str, path: str, body: dict | None = None, timeout: float = 60.0) -> dict:
    """Call the control-plane API as the admin client (Basic auth); the API audits every mutation."""
    user, password = os.environ["LAKEFLOW_ADMIN_USER"], os.environ["LAKEFLOW_ADMIN_PASSWORD"]
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    request = urllib.request.Request(  # noqa: S310 - fixed internal service URL
        os.environ.get("LAKEFLOW_API_URL", "http://api:8000") + path,
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Basic {token}"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
        return json.loads(response.read() or b"{}")


def pg_connect():
    import psycopg2  # noqa: PLC0415 - ships with the Airflow image (postgres provider)

    return psycopg2.connect(os.environ["LAKEFLOW_PG_DSN"])
