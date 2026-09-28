"""Canonical JSON, deterministic event identity, timestamp normalization and PII handling."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from datetime import datetime, timezone
from typing import Any

from .contracts import FieldSpec

_DATETIME = re.compile(r"^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def event_id(source_table: str, primary_key: str, lsn: int, tx_id: int | None, op: str, row_hash: str) -> str:
    """Stable across Debezium redelivery: excludes Kafka coordinates and connector processing time."""
    return sha256_hex(f"{source_table}|{primary_key}|{lsn}|{tx_id if tx_id is not None else ''}|{op}|{row_hash}")


def row_hash(before: Any, after: Any) -> str:
    return sha256_hex(canonical_json({"before": before, "after": after}))


def parse_timestamp(value: str) -> datetime:
    """Parse RFC 3339 with 0-9 fractional digits (Python 3.10's fromisoformat cannot)."""
    match = _DATETIME.match(value)
    if not match:
        raise ValueError(f"not an RFC 3339 timestamp: {value!r}")
    date, clock, fraction, zone = match.groups()
    micros = (fraction or "").ljust(6, "0")[:6]
    zone = "+00:00" if zone == "Z" else zone
    return datetime.fromisoformat(f"{date}T{clock}.{micros}{zone}").astimezone(timezone.utc)


def normalize_timestamp(value: str) -> str:
    return parse_timestamp(value).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def pii_hmac(value: str, key: bytes) -> str:
    return hmac.new(key, value.strip().lower().encode("utf-8"), hashlib.sha256).hexdigest()


def normalize_image(image: dict | None, fields: dict[str, FieldSpec], pii_key: bytes) -> dict | None:
    """Contract-known fields only (unknown fields may carry unclassified PII), UTC timestamps, PII applied."""
    if image is None:
        return None
    out: dict[str, Any] = {}
    for name, spec in fields.items():
        if name not in image:
            continue
        value = image[name]
        column = spec.target_column
        if column is None:
            continue
        if value is not None and spec.fmt == "date-time" and isinstance(value, str):
            value = normalize_timestamp(value)
        if value is not None and spec.pii_handling == "hash":
            value = pii_hmac(str(value), pii_key)
        out[column] = value
    return out
