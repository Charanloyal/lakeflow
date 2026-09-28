"""FastAPI app tests with fake dependencies: auth, CSRF, validation, degradation, rate limits, OpenAPI."""

import base64
from contextlib import contextmanager
from pathlib import Path

import psycopg
import pytest
from fastapi.testclient import TestClient

from lakeflow_api.context import AppContext
from lakeflow_api.main import create_app
from lakeflow_api.settings import Settings
from lakeflow_api.tracer import Broadcaster

ROOT = Path(__file__).resolve().parents[2]
ADMIN = ("admin", "admin-pw-for-tests")
VIEWER = ("viewer", "viewer-pw-for-tests")
ORDER_ID = "3f2b8f7e-5d1a-4c9b-9f60-1a2b3c4d5e6f"


def basic(user):
    return {"Authorization": "Basic " + base64.b64encode(f"{user[0]}:{user[1]}".encode()).decode()}


class FakeClients:
    def __init__(self):
        self.produced = []
        self.http = None

    def trino(self, sql, params=None, timeout_s=20.0):
        raise psycopg.OperationalError("fake: lakehouse down")

    @contextmanager
    def source(self):
        raise psycopg.OperationalError("fake: source database down")
        yield

    @contextmanager
    def control(self):
        raise psycopg.OperationalError("fake: control database down")
        yield

    def produce(self, topic, key, value, headers):
        self.produced.append((topic, key, value, headers))


class FakeTracer:
    running, error, last_heartbeat_ms = False, "fake tracer", None

    def events_for(self, topic, key):
        return []

    def recent(self, limit=50, topic=None):
        return []

    def rate(self, window_s=60.0):
        return 0.0, 0


class FakeMonitor:
    lag, lag_as_of, slot, end_offsets = {}, None, {}, {}

    def snapshot(self):
        return {}


class FakeStore:
    def __init__(self):
        self.audited = []

    def incidents(self, limit=20):
        return []

    def audit(self, actor, action, detail, action_id=None, status="done"):
        self.audited.append((actor, action))

    def audit_entries(self, prefix="", limit=30):
        return []

    def open_incident(self, *args):
        pass

    def latest_mutation(self, table, key):
        return None


def make_client(tmp_path, **overrides):
    values = dict(
        pg_dsn="postgresql://fake", control_dsn="postgresql://fake", kafka_bootstrap="fake:9092",
        connect_url="http://fake", trino_host="fake", trino_port=8080, prometheus_url="http://fake",
        spark_metrics_url="http://fake/metrics", checkpoint_dir=str(tmp_path / "checkpoints"),
        control_dir=str(tmp_path / "control"), contracts_dir=str(ROOT / "contracts"), adr_dir=str(ROOT / "docs" / "adr"),
        benchmark_dir=str(tmp_path / "results"), migrations_dir=str(ROOT / "platform" / "postgres" / "migrations"),
        users={ADMIN[0]: (ADMIN[1], "admin"), VIEWER[0]: (VIEWER[1], "viewer")},
        session_secret="test-session-secret-0123456789", recovery_lab_enabled=True,
        cors_origins=("http://localhost:3000",), start_background=False,
    )
    values.update(overrides)
    settings = Settings(**values)
    clients = FakeClients()
    ctx = AppContext(settings, clients, FakeStore(), FakeTracer(), FakeMonitor(), Broadcaster())
    return TestClient(create_app(settings, ctx)), ctx


@pytest.fixture
def client(tmp_path):
    return make_client(tmp_path)[0]


def test_health_is_public_and_hardened(client):
    response = client.get("/api/health")
    assert response.status_code == 200 and response.json()["status"] == "ok"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"


def test_authentication_is_required_and_never_prompts(client):
    response = client.get("/api/metrics/overview")
    assert response.status_code == 401
    assert "www-authenticate" not in response.headers
    assert client.get("/api/metrics/overview", headers=basic(("admin", "wrong"))).status_code == 401


def test_overview_degrades_with_sources_and_timestamps(client):
    body = client.get("/api/metrics/overview", headers=basic(VIEWER)).json()
    assert body["metrics"], "overview must list metrics even when dependencies are down"
    for metric in body["metrics"]:
        assert metric["source"], metric
        if metric["value"] is None:
            assert metric["stale"] or metric["note"], f"missing value must be explained: {metric}"
    assert body["sla"][0]["status"] == "no_data"


def test_viewer_cannot_mutate(client):
    response = client.post("/api/demo/orders", json={"amount": "10.00"}, headers=basic(VIEWER))
    assert response.status_code == 403


@pytest.mark.parametrize("payload", [
    {"amount": "-1.00"}, {"amount": "1.234"}, {"currency": "JPY"}, {"status": "SHIPPED_TO_MARS"}, {"unexpected": 1},
])
def test_mutation_input_validation(client, payload):
    assert client.post("/api/demo/orders", json=payload, headers=basic(ADMIN)).status_code == 422


def test_update_requires_a_field_and_valid_uuid(client):
    assert client.patch(f"/api/demo/orders/{ORDER_ID}", json={}, headers=basic(ADMIN)).status_code == 422
    assert client.patch("/api/demo/orders/not-a-uuid", json={"status": "PAID"}, headers=basic(ADMIN)).status_code == 422


def test_dependency_outage_is_a_503(client):
    response = client.post("/api/demo/orders", json={"amount": "10.00"}, headers=basic(ADMIN))
    assert response.status_code == 503
    assert "PostgreSQL unavailable" in response.json()["detail"]


def test_session_cookie_requires_csrf_header_for_writes(client):
    login = client.post("/api/auth/login", json={"username": ADMIN[0], "password": ADMIN[1]})
    assert login.status_code == 200 and login.json()["role"] == "admin"
    cookie = login.headers["set-cookie"].lower()
    assert "httponly" in cookie and "samesite=strict" in cookie
    assert client.get("/api/auth/me").json()["user"] == "admin"
    no_csrf = client.post("/api/recovery/actions", json={"action": "inject_malformed", "count": 1})
    assert no_csrf.status_code == 403
    with_csrf = client.post("/api/recovery/actions", json={"action": "inject_malformed", "count": 1},
                            headers={"X-LakeFlow-CSRF": "1"})
    assert with_csrf.status_code == 202


def test_login_rejects_bad_password(client):
    assert client.post("/api/auth/login", json={"username": "admin", "password": "nope"}).status_code == 401


def test_recovery_injection_is_labelled_and_rate_limited(tmp_path):
    client, ctx = make_client(tmp_path)
    for _ in range(10):
        response = client.post("/api/recovery/actions", json={"action": "inject_malformed", "kind": "invalid_json"},
                               headers=basic(ADMIN))
        assert response.status_code == 202
    assert client.post("/api/recovery/actions", json={"action": "inject_malformed"}, headers=basic(ADMIN)).status_code == 429
    assert all("lakeflow-injection-id" in headers for _, _, _, headers in ctx.clients.produced)


def test_recovery_can_be_disabled(tmp_path):
    client, _ = make_client(tmp_path, recovery_lab_enabled=False)
    response = client.post("/api/recovery/actions", json={"action": "crash_now"}, headers=basic(ADMIN))
    assert response.status_code == 403


def test_crash_request_writes_control_file(tmp_path):
    client, _ = make_client(tmp_path)
    response = client.post("/api/recovery/actions", json={"action": "crash_after_commit"}, headers=basic(ADMIN))
    assert response.status_code == 202 and response.json()["status"] == "requested"
    assert list((tmp_path / "control" / "requests").glob("*.json"))


@pytest.mark.parametrize("path", [
    "/api/events?op=x", "/api/events?limit=5000", "/api/trace/orders/bad;key", "/api/lineage/impact?dataset=a%20b",
])
def test_query_validation(client, path):
    assert client.get(path, headers=basic(VIEWER)).status_code == 422


def test_architecture_and_lineage_are_served(client):
    adrs = client.get("/api/architecture/adrs", headers=basic(VIEWER)).json()["adrs"]
    assert len(adrs) >= 5 and adrs[2]["title"].startswith("End-to-end delivery")
    lineage = client.get("/api/lineage", headers=basic(VIEWER)).json()
    assert any(d["id"] == "lakehouse.silver.orders" for d in lineage["datasets"])
    impact = client.get("/api/lineage/impact?dataset=postgres.shop.orders", headers=basic(VIEWER)).json()
    assert "lakehouse.silver.orders" in impact["affected_datasets"]


def test_benchmarks_empty_directory(client):
    assert client.get("/api/benchmarks/runs", headers=basic(VIEWER)).json()["runs"] == []


def test_cors_allows_only_configured_origins(client):
    ok = client.options("/api/health", headers={"Origin": "http://localhost:3000", "Access-Control-Request-Method": "GET"})
    assert ok.headers.get("access-control-allow-origin") == "http://localhost:3000"
    bad = client.options("/api/health", headers={"Origin": "https://evil.example", "Access-Control-Request-Method": "GET"})
    assert "access-control-allow-origin" not in bad.headers


def test_openapi_document_is_generated(client):
    spec = client.get("/api/openapi.json").json()
    assert "/api/trace/{table}/{key}" in spec["paths"]
    assert "/api/recovery/actions" in spec["paths"]
