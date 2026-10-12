"""Display-safe views of data that bypasses the contract's PII handling.

Two places hold row values *before* contract PII handling:

* ``ops.dlq_events.raw_key`` / ``raw_value``: the exact rejected bytes. Replay needs them unchanged, so the table is
  a restricted quarantine and is never rewritten here.
* the PostgreSQL source row shown next to the lakehouse state in a record trace.

Anything that leaves the platform for display (API responses, the UI, the recorded demo) goes through this module.
It masks every field a contract classifies as direct PII and every field the contract does not know (unknown
fields may carry unclassified PII, the same rule ``identity.normalize_image`` applies before silver). A payload
that cannot be masked field by field (unparseable, binary, or not a Debezium envelope) is withheld when its contract
has direct-PII fields or is unknown.
"""

from __future__ import annotations

import json
from typing import Any

from .contracts import ContractRegistry

REDACTED_PII = "[redacted: direct PII]"
REDACTED_UNCLASSIFIED = "[redacted: field not in contract]"
REDACTED_NOT_OBJECT = "[redacted: row image is not an object]"
WITHHELD = "[withheld: {reason}; the exact bytes stay in ops.dlq_events for replay]"

REDACTION_NONE = "none"
REDACTION_MASKED = "masked"
REDACTION_WITHHELD = "withheld"
# Top-level keys of a Debezium change event (JsonConverter, schemas disabled). Row data lives only in before/after.
ENVELOPE_KEYS = frozenset({"before", "after", "source", "op", "ts_ms", "ts_us", "ts_ns", "transaction"})


def direct_pii_fields(registry: ContractRegistry, contract_name: str | None) -> frozenset[str] | None:
    """Direct-PII field names across every active version, or None when the contract is unknown."""
    if contract_name is None or contract_name not in registry.names:
        return None
    return frozenset(name for name, spec in registry.all_fields(contract_name).items() if spec.pii == "direct")


def mask_row(row: dict | None, registry: ContractRegistry, contract_name: str) -> tuple[dict | None, bool]:
    """Mask direct-PII and unclassified fields of one row image. Returns (row, changed)."""
    if row is None:
        return None, False
    known = registry.all_fields(contract_name)
    pii = direct_pii_fields(registry, contract_name) or frozenset()
    out: dict[str, Any] = {}
    changed = False
    for name, value in row.items():
        if name not in known:
            out[name], changed = REDACTED_UNCLASSIFIED, True
        elif name in pii and value is not None:
            out[name], changed = REDACTED_PII, True
        else:
            out[name] = value
    return out, changed


def redact_dlq_payload(
    raw: str | None, contract_name: str | None, registry: ContractRegistry, max_chars: int = 2000
) -> tuple[str | None, str]:
    """Display text for a DLQ ``raw_value`` and how it was redacted (none / masked / withheld).

    Payloads of contracts without direct PII are returned unchanged (only truncated), so the triage view keeps
    showing the exact rejected bytes for them.
    """
    if raw is None:
        return None, REDACTION_NONE
    pii = direct_pii_fields(registry, contract_name)
    if pii is None:
        return WITHHELD.format(reason="unknown contract, PII classification unavailable"), REDACTION_WITHHELD
    if not pii:
        return raw[:max_chars], REDACTION_NONE
    if raw.startswith("base64:"):
        return WITHHELD.format(reason="binary payload of a contract with direct PII"), REDACTION_WITHHELD
    try:
        doc = json.loads(raw)
    except ValueError:
        return WITHHELD.format(reason="unparseable payload of a contract with direct PII"), REDACTION_WITHHELD
    if not isinstance(doc, dict):
        return WITHHELD.format(reason="payload of a contract with direct PII is not a JSON object"), REDACTION_WITHHELD
    if not isinstance(doc.get("op"), str) or not set(doc) <= ENVELOPE_KEYS:
        # Only the Debezium envelope shape is masked field by field; flat rows, schema-wrapped envelopes and
        # anything else could carry PII anywhere, so they fail closed.
        reason = "payload of a contract with direct PII is not a Debezium envelope"
        return WITHHELD.format(reason=reason), REDACTION_WITHHELD

    changed = False
    for side in ("before", "after"):
        image = doc.get(side)
        if image is None:
            continue
        if not isinstance(image, dict):
            doc[side], changed = REDACTED_NOT_OBJECT, True
            continue
        doc[side], masked = mask_row(image, registry, contract_name)
        changed = changed or masked
    if not changed:
        return raw[:max_chars], REDACTION_NONE
    return json.dumps(doc, ensure_ascii=False, separators=(",", ":"))[:max_chars], REDACTION_MASKED
