"""Thin, timeout-bounded clients for the platform services. Failures raise; callers decide how to degrade."""

from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal

import httpx
import psycopg
import trino
from confluent_kafka import Consumer, Producer, TopicPartition
from psycopg.rows import dict_row

from .settings import Settings


class Clients:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.http = httpx.Client(timeout=httpx.Timeout(5.0, connect=2.0))
        self._producer: Producer | None = None

    # ---------------------------------------------------------------------------------------- PostgreSQL
    @contextmanager
    def source(self):
        with psycopg.connect(self.settings.pg_dsn, connect_timeout=3, row_factory=dict_row) as conn:
            yield conn

    @contextmanager
    def control(self):
        with psycopg.connect(
            self.settings.control_dsn, connect_timeout=3, row_factory=dict_row, autocommit=True
        ) as conn:
            yield conn

    # --------------------------------------------------------------------------------------------- Trino
    def trino(self, sql: str, params: tuple | list | None = None, timeout_s: float = 20.0) -> tuple[list[dict], float]:
        """Run one statement; returns (rows as dicts, elapsed ms). Values are JSON-friendly."""
        started = time.perf_counter()
        conn = trino.dbapi.connect(
            host=self.settings.trino_host,
            port=self.settings.trino_port,
            user="lakeflow-api",
            catalog="lakehouse",
            schema="silver",
            request_timeout=timeout_s,
        )
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            rows = cursor.fetchall()
            names = [d[0] for d in cursor.description or []]
        finally:
            conn.close()
        elapsed = (time.perf_counter() - started) * 1000
        return [{n: _plain(v) for n, v in zip(names, row, strict=True)} for row in rows], elapsed

    # ------------------------------------------------------------------------------------- Kafka Connect
    def connector_status(self) -> dict:
        response = self.http.get(f"{self.settings.connect_url}/connectors/{self.settings.connector_name}/status")
        response.raise_for_status()
        return response.json()

    def restart_connector(self) -> None:
        response = self.http.post(
            f"{self.settings.connect_url}/connectors/{self.settings.connector_name}/restart",
            params={"includeTasks": "true", "onlyFailed": "false"},
        )
        response.raise_for_status()

    # --------------------------------------------------------------------------------------------- Kafka
    def producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(
                {
                    "bootstrap.servers": self.settings.kafka_bootstrap,
                    "acks": "all",
                    "enable.idempotence": True,
                    "client.id": "lakeflow-api",
                }
            )
        return self._producer

    def produce(self, topic: str, key: bytes | None, value: bytes | None, headers: dict[str, str]) -> None:
        producer = self.producer()
        producer.produce(topic, key=key, value=value, headers=[(k, v.encode()) for k, v in headers.items()])
        remaining = producer.flush(10)
        if remaining:
            raise RuntimeError(f"{remaining} Kafka messages were not delivered within 10s")

    def end_offsets(self, topics: tuple[str, ...]) -> dict[str, dict[str, int]]:
        consumer = Consumer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap,
                "group.id": "lakeflow-api-probe",
                "enable.auto.commit": False,
            }
        )
        try:
            metadata = consumer.list_topics(timeout=5)
            out: dict[str, dict[str, int]] = {}
            for topic in topics:
                if topic not in metadata.topics:
                    continue
                for partition in metadata.topics[topic].partitions:
                    _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=5)
                    out.setdefault(topic, {})[str(partition)] = int(high)
            return out
        finally:
            consumer.close()

    def fetch_record(self, topic: str, partition: int, offset: int) -> tuple[bytes | None, bytes | None]:
        """Read one exact record (used to redeliver a real event verbatim)."""
        consumer = Consumer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap,
                "group.id": "lakeflow-api-fetch",
                "enable.auto.commit": False,
            }
        )
        try:
            consumer.assign([TopicPartition(topic, partition, offset)])
            deadline = time.time() + 10
            while time.time() < deadline:
                message = consumer.poll(1.0)
                if message is None or message.error():
                    continue
                if message.offset() == offset:
                    return message.key(), message.value()
            raise RuntimeError(f"record {topic}/{partition}/{offset} not readable (retention?)")
        finally:
            consumer.close()


def _plain(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value
