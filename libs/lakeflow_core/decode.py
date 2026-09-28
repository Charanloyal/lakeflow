"""Decode one Kafka record carrying a Debezium change event into a flat, validated structure.

This is the only decoder in the project: the Spark job wraps it in a UDF, and the tests and the API call it
directly, so every consumer classifies events the same way. Outcomes:

* ``tombstone``: Kafka null value (Debezium emits one after each delete when tombstones.on.delete=true).
  Counted and skipped because the preceding ``op=d`` event already carries the delete.
* ``dlq``: undecodable or contract-violating. ``error_code`` explains why, and ``raw_*`` keeps the payload
  for replay.
* ``valid``: normalized row images plus a deterministic ``event_id``.
"""

from __future__ import annotations

import base64
import json
from typing import Any

from .contracts import ContractRegistry
from .identity import canonical_json, event_id, normalize_image, row_hash

STATUS_VALID = "valid"
STATUS_DLQ = "dlq"
STATUS_TOMBSTONE = "tombstone"

SUPPORTED_OPS = {"c", "u", "d", "r"}

# (column, Spark SQL type): the Spark UDF return type is generated from this list so the two cannot drift.
DECODED_SCHEMA: tuple[tuple[str, str], ...] = (
    ("status", "string"),
    ("error_code", "string"),
    ("error_detail", "string"),
    ("violations", "array<string>"),
    ("raw_key", "string"),
    ("raw_value", "string"),
    ("source_table", "string"),
    ("contract_name", "string"),
    ("contract_version", "int"),
    ("drift_fields", "array<string>"),
    ("op", "string"),
    ("primary_key", "string"),
    ("source_lsn", "bigint"),
    ("source_tx_id", "bigint"),
    ("source_ts_ms", "bigint"),
    ("source_snapshot", "string"),
    ("debezium_ts_ms", "bigint"),
    ("before_json", "string"),
    ("after_json", "string"),
    ("event_id", "string"),
)


def _empty() -> dict[str, Any]:
    return {name: None for name, _ in DECODED_SCHEMA}


def _raw_text(data: bytes | None) -> str | None:
    if data is None:
        return None
    try:
        return bytes(data).decode("utf-8")
    except UnicodeDecodeError:
        return "base64:" + base64.b64encode(bytes(data)).decode("ascii")


def _dlq(result: dict[str, Any], code: str, detail: str, key: bytes | None, value: bytes | None, violations=()):
    result.update(
        status=STATUS_DLQ,
        error_code=code,
        error_detail=detail[:1000],
        violations=list(violations),
        raw_key=_raw_text(key),
        raw_value=_raw_text(value),
    )
    return result


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def decode_change_event(
    topic: str,
    key: bytes | None,
    value: bytes | None,
    registry: ContractRegistry,
    pii_key: bytes,
) -> dict[str, Any]:
    """Never raises: a poison-pill record must end up in the DLQ, not crash the micro-batch."""
    try:
        return _decode(topic, key, value, registry, pii_key)
    except Exception as exc:  # noqa: BLE001 - total function by design
        return _dlq(_empty(), "DECODER_ERROR", f"{type(exc).__name__}: {exc}", key, value)


def _decode(
    topic: str,
    key: bytes | None,
    value: bytes | None,
    registry: ContractRegistry,
    pii_key: bytes,
) -> dict[str, Any]:
    result = _empty()
    if value is None:
        result["status"] = STATUS_TOMBSTONE
        return result

    contract_name = registry.name_for_topic(topic)
    if contract_name is None:
        return _dlq(result, "UNKNOWN_TOPIC", f"no contract is registered for topic {topic!r}", key, value)
    result["contract_name"] = contract_name
    contract = registry.current(contract_name)
    result["source_table"] = contract.source_table

    try:
        text = bytes(value).decode("utf-8")
    except UnicodeDecodeError as exc:
        return _dlq(result, "MALFORMED_ENCODING", f"value is not UTF-8: {exc}", key, value)
    try:
        envelope = json.loads(text)
    except json.JSONDecodeError as exc:
        return _dlq(result, "MALFORMED_JSON", f"value is not JSON: {exc.msg} at char {exc.pos}", key, value)
    if not isinstance(envelope, dict):
        return _dlq(result, "INVALID_ENVELOPE", "envelope must be a JSON object", key, value)

    op = envelope.get("op")
    if not isinstance(op, str):
        return _dlq(result, "INVALID_ENVELOPE", "envelope.op is missing", key, value)
    if op not in SUPPORTED_OPS:
        return _dlq(result, "UNSUPPORTED_OP", f"op {op!r} is not applied by this pipeline (truncate/message)", key, value)

    source = envelope.get("source")
    if not isinstance(source, dict):
        return _dlq(result, "INVALID_ENVELOPE", "envelope.source is missing", key, value)
    lsn, ts_ms, tx_id = source.get("lsn"), source.get("ts_ms"), source.get("txId")
    if not _is_int(lsn) or lsn < 0 or not _is_int(ts_ms) or (tx_id is not None and not _is_int(tx_id)):
        return _dlq(result, "INVALID_ENVELOPE", "source.lsn/ts_ms must be integers (txId integer or null)", key, value)
    envelope_table = f"{source.get('schema')}.{source.get('table')}"
    if envelope_table != contract.source_table:
        detail = f"source {envelope_table!r} does not match contract table {contract.source_table!r}"
        return _dlq(result, "INVALID_ENVELOPE", detail, key, value)

    before, after = envelope.get("before"), envelope.get("after")
    if (before is not None and not isinstance(before, dict)) or (after is not None and not isinstance(after, dict)):
        return _dlq(result, "INVALID_ENVELOPE", "before/after must be objects or null", key, value)
    image = before if op == "d" else after
    if image is None:
        side = "before" if op == "d" else "after"
        return _dlq(result, "MISSING_ROW_IMAGE", f"op={op} requires envelope.{side}", key, value)

    classification = registry.classify(contract_name, image)
    if not classification.valid:
        codes = [v.code for v in classification.violations]
        detail = "; ".join(v.message for v in classification.violations)
        return _dlq(result, "CONTRACT_VIOLATION", detail, key, value, violations=codes)

    pk_values = [image.get(name) for name in contract.primary_key]
    primary_key = "|".join(str(v) for v in pk_values)

    if key is not None:
        try:
            key_doc = json.loads(bytes(key).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return _dlq(result, "KEY_MISMATCH", "record key is not JSON", key, value)
        if isinstance(key_doc, dict) and "payload" in key_doc and "schema" in key_doc:
            key_doc = key_doc["payload"]
        if not isinstance(key_doc, dict) or [str(key_doc.get(n)) for n in contract.primary_key] != [
            str(v) for v in pk_values
        ]:
            return _dlq(result, "KEY_MISMATCH", "record key does not match the row image primary key", key, value)

    fields = registry.all_fields(contract_name)
    try:
        before_json = _dump(normalize_image(before, fields, pii_key))
        after_json = _dump(normalize_image(after, fields, pii_key))
    except ValueError as exc:
        return _dlq(result, "MALFORMED_ROW_IMAGE", f"row image cannot be normalized: {exc}", key, value)
    snapshot = source.get("snapshot")
    debezium_ts = envelope.get("ts_ms")
    result.update(
        status=STATUS_VALID,
        contract_version=classification.version,
        drift_fields=list(classification.drift_fields),
        violations=[],
        op=op,
        primary_key=primary_key,
        source_lsn=lsn,
        source_tx_id=tx_id,
        source_ts_ms=ts_ms,
        source_snapshot=None if snapshot is None else str(snapshot).lower(),
        debezium_ts_ms=debezium_ts if _is_int(debezium_ts) else None,
        before_json=before_json,
        after_json=after_json,
        event_id=event_id(contract.source_table, primary_key, lsn, tx_id, op, row_hash(before, after)),
    )
    return result


def _dump(image: dict | None) -> str | None:
    return None if image is None else canonical_json(image)
