from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise SystemExit(f"FATAL: environment variable {name} is required (see .env.example)")
    return value


@dataclass(frozen=True)
class Settings:
    pipeline: str = "lakeflow_cdc"
    catalog: str = "lakehouse"
    kafka_bootstrap: str = "kafka:9092"
    topics: tuple[str, ...] = ("lakeflow.shop.orders", "lakeflow.shop.customers", "lakeflow.replay.cdc")
    ops_topic: str = "lakeflow.ops.batches"
    catalog_uri: str = "http://iceberg-rest:8181"
    warehouse: str = "s3://warehouse/"
    s3_endpoint: str = "http://minio:9000"
    checkpoint_dir: str = "/checkpoints/lakeflow_cdc"
    control_dir: str = "/control"
    contracts_dir: str = "/opt/lakeflow/contracts"
    trigger_seconds: int = 5
    max_offsets_per_trigger: int = 20000
    allowed_lateness_ms: int = 300_000
    starting_offsets: str = "earliest"
    fail_on_data_loss: bool = True
    shuffle_partitions: int = 4
    metrics_port: int = 9108
    pii_key: bytes = field(default=b"", repr=False)

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap=_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            topics=tuple(t.strip() for t in _env("CDC_TOPICS", ",".join(cls.topics)).split(",") if t.strip()),
            ops_topic=_env("OPS_BATCH_TOPIC", cls.ops_topic),
            catalog_uri=_env("ICEBERG_CATALOG_URI", cls.catalog_uri),
            warehouse=_env("ICEBERG_WAREHOUSE", cls.warehouse),
            s3_endpoint=_env("S3_ENDPOINT", cls.s3_endpoint),
            checkpoint_dir=_env("CHECKPOINT_DIR", cls.checkpoint_dir),
            control_dir=_env("CONTROL_DIR", cls.control_dir),
            contracts_dir=_env("CONTRACTS_DIR", cls.contracts_dir),
            trigger_seconds=int(_env("TRIGGER_INTERVAL_SECONDS", "5")),
            max_offsets_per_trigger=int(_env("MAX_OFFSETS_PER_TRIGGER", "20000")),
            allowed_lateness_ms=int(_env("ALLOWED_LATENESS_SECONDS", "300")) * 1000,
            starting_offsets=_env("STARTING_OFFSETS", "earliest"),
            fail_on_data_loss=_env("FAIL_ON_DATA_LOSS", "true").lower() == "true",
            shuffle_partitions=int(_env("SPARK_SHUFFLE_PARTITIONS", "4")),
            metrics_port=int(_env("METRICS_PORT", "9108")),
            pii_key=_env("LAKEFLOW_PII_HMAC_KEY").encode("utf-8"),
        )
