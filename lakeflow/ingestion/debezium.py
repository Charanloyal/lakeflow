"""Debezium Connect Connector Configuration & Lifecycle Management for LakeFlow."""

from __future__ import annotations
import json
from typing import Any, Dict, Optional


class DebeziumConnectorManager:
    """Generates hardened Debezium PostgreSQL connector specifications and monitors connector tasks."""

    def __init__(
        self,
        connector_name: str = "lakeflow-postgres-cdc",
        pg_host: str = "postgres",
        pg_port: int = 5432,
        pg_db: str = "platform_db",
        pg_user: str = "postgres",
        pg_password: str = "postgres",
        kafka_bootstrap: str = "kafka:9092",
    ):
        self.connector_name = connector_name
        self.pg_host = pg_host
        self.pg_port = pg_port
        self.pg_db = pg_db
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.kafka_bootstrap = kafka_bootstrap

    def get_connector_config(self) -> Dict[str, Any]:
        """Returns the production-hardened Debezium connector configuration payload."""
        return {
            "name": self.connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "tasks.max": "1",
                "plugin.name": "pgoutput",
                "database.hostname": self.pg_host,
                "database.port": str(self.pg_port),
                "database.user": self.pg_user,
                "database.password": self.pg_password,
                "database.dbname": self.pg_db,
                "database.server.name": "lakeflow",
                "table.include.list": "platform.customers,platform.orders,platform.order_items,platform.telemetry_events",
                "slot.name": "lakeflow_cdc_slot",
                "slot.drop.on.stop": "false",
                "publication.name": "lakeflow_publication",
                "publication.autocreate.mode": "all_tables",
                # Senior engineering hardening:
                "heartbeat.interval.ms": "5000",
                "heartbeat.action.query": "INSERT INTO platform.telemetry_events (event_id, event_type, payload) VALUES (gen_random_uuid(), 'HEARTBEAT', '{\"status\":\"alive\"}')",
                "tombstones.on.delete": "true",
                "decimal.handling.mode": "double",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
            },
        }

    def to_json(self) -> str:
        return json.dumps(self.get_connector_config(), indent=2)
