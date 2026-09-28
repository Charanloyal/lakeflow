from __future__ import annotations

import os
from dataclasses import dataclass, field


def _required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"{name} is not set; run `make bootstrap` to generate .env (see .env.example)")
    return value


@dataclass(frozen=True)
class Settings:
    pg_dsn: str
    control_dsn: str
    kafka_bootstrap: str
    connect_url: str
    trino_host: str
    trino_port: int
    prometheus_url: str
    spark_metrics_url: str
    checkpoint_dir: str
    control_dir: str
    contracts_dir: str
    adr_dir: str
    benchmark_dir: str
    migrations_dir: str
    users: dict[str, tuple[str, str]] = field(repr=False)
    session_secret: str = field(repr=False)
    recovery_lab_enabled: bool = True
    cors_origins: tuple[str, ...] = ()
    environment: str = "local-demo"
    connector_name: str = "lakeflow-cdc"
    cdc_topics: tuple[str, ...] = ("lakeflow.shop.orders", "lakeflow.shop.customers")
    replay_topic: str = "lakeflow.replay.cdc"
    ops_topic: str = "lakeflow.ops.batches"
    heartbeat_topic: str = "__debezium-heartbeat.lakeflow"
    start_background: bool = True

    @classmethod
    def from_env(cls) -> Settings:
        admin, viewer = os.environ.get("LAKEFLOW_ADMIN_USER", "admin"), os.environ.get("LAKEFLOW_VIEWER_USER", "viewer")
        if admin == viewer:
            raise RuntimeError("LAKEFLOW_ADMIN_USER and LAKEFLOW_VIEWER_USER must differ")
        secret = _required("LAKEFLOW_SESSION_SECRET")
        if len(secret) < 16:
            raise RuntimeError("LAKEFLOW_SESSION_SECRET must be at least 16 characters")
        return cls(
            pg_dsn=_required("LAKEFLOW_PG_DSN"),
            control_dsn=_required("LAKEFLOW_CONTROL_DSN"),
            kafka_bootstrap=os.environ.get("LAKEFLOW_KAFKA_BOOTSTRAP", "kafka:9092"),
            connect_url=os.environ.get("LAKEFLOW_CONNECT_URL", "http://connect:8083"),
            trino_host=os.environ.get("LAKEFLOW_TRINO_HOST", "trino"),
            trino_port=int(os.environ.get("LAKEFLOW_TRINO_PORT", "8080")),
            prometheus_url=os.environ.get("LAKEFLOW_PROMETHEUS_URL", "http://prometheus:9090"),
            spark_metrics_url=os.environ.get("LAKEFLOW_SPARK_METRICS_URL", "http://spark:9108/metrics"),
            checkpoint_dir=os.environ.get("LAKEFLOW_CHECKPOINT_DIR", "/checkpoints/lakeflow_cdc"),
            control_dir=os.environ.get("LAKEFLOW_CONTROL_DIR", "/control"),
            contracts_dir=os.environ.get("LAKEFLOW_CONTRACTS_DIR", "/app/contracts"),
            adr_dir=os.environ.get("LAKEFLOW_ADR_DIR", "/app/docs/adr"),
            benchmark_dir=os.environ.get("LAKEFLOW_BENCHMARK_DIR", "/app/benchmarks/results"),
            migrations_dir=os.environ.get("LAKEFLOW_MIGRATIONS_DIR", "/migrations"),
            users={
                admin: (_required("LAKEFLOW_ADMIN_PASSWORD"), "admin"),
                viewer: (_required("LAKEFLOW_VIEWER_PASSWORD"), "viewer"),
            },
            session_secret=secret,
            recovery_lab_enabled=os.environ.get("LAKEFLOW_RECOVERY_LAB_ENABLED", "true").lower() == "true",
            cors_origins=tuple(o.strip() for o in os.environ.get("LAKEFLOW_CORS_ORIGINS", "").split(",") if o.strip()),
            environment=os.environ.get("LAKEFLOW_ENVIRONMENT", "local-demo"),
            start_background=os.environ.get("LAKEFLOW_START_BACKGROUND", "true").lower() == "true",
        )
