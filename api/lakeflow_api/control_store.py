"""Control database (incidents, audit log, mutation log) owned by the API."""

from __future__ import annotations

import json
import logging

log = logging.getLogger("lakeflow.control")

SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
    id bigserial PRIMARY KEY, opened_at timestamptz NOT NULL DEFAULT now(), resolved_at timestamptz,
    component text NOT NULL, severity text NOT NULL, title text NOT NULL, detail text, source text NOT NULL);
CREATE TABLE IF NOT EXISTS audit_log (
    id bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now(), actor text NOT NULL, action text NOT NULL,
    action_id text, status text NOT NULL DEFAULT 'done', detail jsonb NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS mutations (
    id bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now(), actor text NOT NULL, table_name text NOT NULL,
    key text NOT NULL, op text NOT NULL, txid bigint, commit_lsn text, detail jsonb NOT NULL DEFAULT '{}');
CREATE INDEX IF NOT EXISTS mutations_key_idx ON mutations (table_name, key, id DESC);
"""


class ControlStore:
    def __init__(self, clients):
        self.clients = clients

    def init(self) -> None:
        with self.clients.control() as conn:
            conn.execute(SCHEMA)

    def open_incident(self, component: str, severity: str, title: str, detail: str, source: str) -> None:
        with self.clients.control() as conn:
            existing = conn.execute(
                "SELECT id FROM incidents WHERE component = %s AND resolved_at IS NULL AND title = %s",
                (component, title),
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO incidents (component, severity, title, detail, source) VALUES (%s, %s, %s, %s, %s)",
                    (component, severity, title, detail[:2000], source),
                )

    def resolve_incidents(self, component: str) -> None:
        with self.clients.control() as conn:
            conn.execute("UPDATE incidents SET resolved_at = now() WHERE component = %s AND resolved_at IS NULL", (component,))

    def incidents(self, limit: int = 20) -> list[dict]:
        with self.clients.control() as conn:
            return list(conn.execute("SELECT * FROM incidents ORDER BY opened_at DESC LIMIT %s", (limit,)).fetchall())

    def audit(self, actor: str, action: str, detail: dict, action_id: str | None = None, status: str = "done") -> None:
        with self.clients.control() as conn:
            conn.execute(
                "INSERT INTO audit_log (actor, action, action_id, status, detail) VALUES (%s, %s, %s, %s, %s)",
                (actor, action, action_id, status, json.dumps(detail, default=str)),
            )

    def audit_entries(self, prefix: str = "", limit: int = 30) -> list[dict]:
        with self.clients.control() as conn:
            return list(conn.execute(
                "SELECT * FROM audit_log WHERE action LIKE %s ORDER BY at DESC LIMIT %s", (prefix + "%", limit)
            ).fetchall())

    def record_mutation(self, actor: str, table: str, key: str, op: str, txid, commit_lsn, detail: dict) -> None:
        with self.clients.control() as conn:
            conn.execute(
                "INSERT INTO mutations (actor, table_name, key, op, txid, commit_lsn, detail) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (actor, table, key, op, txid, commit_lsn, json.dumps(detail, default=str)),
            )

    def latest_mutation(self, table: str, key: str) -> dict | None:
        with self.clients.control() as conn:
            return conn.execute(
                "SELECT * FROM mutations WHERE table_name = %s AND key = %s ORDER BY id DESC LIMIT 1", (table, key)
            ).fetchone()
