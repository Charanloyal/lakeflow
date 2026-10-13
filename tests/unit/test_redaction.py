"""Display redaction of data that bypasses contract PII handling (DLQ raw bytes, PostgreSQL source rows).

Every value below is synthetic. The sentinels make a leak easy to spot in an assertion message.
"""

import base64
import json
import unittest
from pathlib import Path

from lakeflow_core.contracts import load_registry
from lakeflow_core.decode import decode_change_event
from lakeflow_core.redaction import (
    REDACTED_PII,
    REDACTED_UNCLASSIFIED,
    REDACTION_MASKED,
    REDACTION_NONE,
    REDACTION_WITHHELD,
    direct_pii_fields,
    mask_row,
    redact_dlq_payload,
)

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = load_registry(ROOT / "contracts")
SYNTHETIC_EMAIL = "synthetic.person.7731@example.invalid"
SYNTHETIC_NAME = "Synthetic Sentinel Person"
CUSTOMER = {
    "customer_id": "0b7e7c1e-2f7a-4c55-8f0e-6a1d2c3b4a59",
    "email": SYNTHETIC_EMAIL,
    "full_name": SYNTHETIC_NAME,
    "country": "DE",
    "created_at": "2026-09-25T10:00:00Z",
    "updated_at": "2026-09-25T10:00:00Z",
}
ORDER = {
    "order_id": "3f2b8f7e-5d1a-4c9b-9f60-1a2b3c4d5e6f",
    "customer_id": CUSTOMER["customer_id"],
    "status": "SHIPPED_TO_MARS",
    "amount": "-1.00",
    "currency": "USD",
    "created_at": "2026-09-25T10:00:00Z",
    "updated_at": "2026-09-25T10:00:00Z",
}


def envelope(table, after, before=None, op="c"):
    source = {"schema": "shop", "table": table, "lsn": 24023184, "txId": 747, "ts_ms": 1790323163123}
    return json.dumps({"before": before, "after": after, "source": source, "op": op, "ts_ms": 1790323163456})


def assert_no_pii(case, text):
    case.assertNotIn(SYNTHETIC_EMAIL, text)
    case.assertNotIn(SYNTHETIC_NAME, text)


class RedactionTests(unittest.TestCase):
    def test_direct_pii_fields_follow_the_contracts(self):
        self.assertEqual(direct_pii_fields(REGISTRY, "customers"), {"email", "full_name"})
        self.assertEqual(direct_pii_fields(REGISTRY, "orders"), frozenset())
        self.assertIsNone(direct_pii_fields(REGISTRY, "unknown"))
        self.assertIsNone(direct_pii_fields(REGISTRY, None))

    def test_contract_without_direct_pii_keeps_the_exact_bytes(self):
        raw = envelope("orders", ORDER)
        self.assertEqual(redact_dlq_payload(raw, "orders", REGISTRY), (raw, REDACTION_NONE))
        malformed = '{"op": "c", "source": {"lsn": 1, '
        self.assertEqual(redact_dlq_payload(malformed, "orders", REGISTRY), (malformed, REDACTION_NONE))

    def test_customer_payload_masks_direct_pii_and_keeps_triage_fields(self):
        raw = envelope("customers", {**CUSTOMER, "email": "not-an-email"})
        text, how = redact_dlq_payload(raw, "customers", REGISTRY)
        self.assertEqual(how, REDACTION_MASKED)
        doc = json.loads(text)
        self.assertEqual((doc["after"]["email"], doc["after"]["full_name"]), (REDACTED_PII, REDACTED_PII))
        self.assertEqual(doc["after"]["customer_id"], CUSTOMER["customer_id"], "pseudonymous key stays for triage")
        self.assertEqual((doc["after"]["country"], doc["op"], doc["source"]["lsn"]), ("DE", "c", 24023184))

    def test_before_and_after_images_are_both_masked(self):
        text, how = redact_dlq_payload(
            envelope("customers", CUSTOMER, before=CUSTOMER, op="u"), "customers", REGISTRY, max_chars=100_000
        )
        self.assertEqual(how, REDACTION_MASKED)
        assert_no_pii(self, text)

    def test_unknown_fields_are_masked_as_unclassified(self):
        text, _ = redact_dlq_payload(
            envelope("customers", {**CUSTOMER, "phone": "+00 0000 000000"}), "customers", REGISTRY
        )
        self.assertEqual(json.loads(text)["after"]["phone"], REDACTED_UNCLASSIFIED)

    def test_unmaskable_payloads_of_pii_contracts_are_withheld(self):
        cases = {
            "unparseable": f'{{"after": {{"email": "{SYNTHETIC_EMAIL}", "full_name": "{SYNTHETIC_NAME}"',
            "binary": "base64:" + base64.b64encode(f"\xff{SYNTHETIC_EMAIL}".encode("latin-1")).decode(),
            "not an object": json.dumps([SYNTHETIC_EMAIL, SYNTHETIC_NAME]),
            "flat row without envelope": json.dumps(CUSTOMER),
            "schema-wrapped envelope": json.dumps({"schema": {}, "payload": {"op": "c", "after": CUSTOMER}}),
            "envelope with unexpected key": json.dumps({"op": "c", "after": None, "extra": CUSTOMER}),
        }
        for label, raw in cases.items():
            with self.subTest(label):
                text, how = redact_dlq_payload(raw, "customers", REGISTRY)
                self.assertEqual(how, REDACTION_WITHHELD)
                assert_no_pii(self, text)
                self.assertIn("ops.dlq_events", text)

    def test_unknown_contract_is_withheld(self):
        text, how = redact_dlq_payload(envelope("customers", CUSTOMER), None, REGISTRY)
        self.assertEqual(how, REDACTION_WITHHELD)
        assert_no_pii(self, text)

    def test_non_object_row_image_is_masked(self):
        raw = json.dumps({"before": None, "after": f"{SYNTHETIC_NAME} <{SYNTHETIC_EMAIL}>", "op": "c"})
        text, how = redact_dlq_payload(raw, "customers", REGISTRY)
        self.assertEqual(how, REDACTION_MASKED)
        assert_no_pii(self, text)

    def test_truncation_happens_after_masking(self):
        text, how = redact_dlq_payload(envelope("customers", CUSTOMER), "customers", REGISTRY, max_chars=40)
        self.assertEqual((len(text), how), (40, REDACTION_MASKED))
        assert_no_pii(self, text)

    def test_none_stays_none(self):
        self.assertEqual(redact_dlq_payload(None, "customers", REGISTRY), (None, REDACTION_NONE))

    def test_source_row_masking(self):
        masked, changed = mask_row(CUSTOMER, REGISTRY, "customers")
        self.assertTrue(changed)
        assert_no_pii(self, json.dumps(masked))
        self.assertEqual(masked["customer_id"], CUSTOMER["customer_id"])
        unchanged, changed = mask_row({**ORDER, "status": "PAID", "amount": "1.00"}, REGISTRY, "orders")
        self.assertFalse(changed)
        self.assertEqual(unchanged["status"], "PAID")
        self.assertEqual(mask_row(None, REGISTRY, "orders"), (None, False))


class DlqPrivacyPathTests(unittest.TestCase):
    """The decoder keeps the exact bytes for replay; only the display copy is masked."""

    def test_rejected_customer_keeps_exact_bytes_for_replay_but_displays_masked(self):
        value = envelope("customers", {**CUSTOMER, "email": SYNTHETIC_EMAIL.replace("@", " at ")})
        key = json.dumps({"customer_id": CUSTOMER["customer_id"]}).encode()
        out = decode_change_event("lakeflow.shop.customers", key, value.encode(), REGISTRY, b"unit-test-pii-key")
        self.assertEqual((out["status"], out["error_code"]), ("dlq", "CONTRACT_VIOLATION"))
        self.assertEqual(out["raw_value"], value, "replay needs the exact rejected bytes")
        self.assertNotIn(SYNTHETIC_NAME, out["error_detail"])
        self.assertNotIn("at example", out["error_detail"])
        text, how = redact_dlq_payload(out["raw_value"], out["contract_name"], REGISTRY)
        self.assertEqual(how, REDACTION_MASKED)
        self.assertNotIn(SYNTHETIC_NAME, text)
        self.assertNotIn("at example", text)

    def test_valid_customer_never_carries_raw_pii_into_bronze_columns(self):
        value = envelope("customers", CUSTOMER).encode()
        key = json.dumps({"customer_id": CUSTOMER["customer_id"]}).encode()
        out = decode_change_event("lakeflow.shop.customers", key, value, REGISTRY, b"unit-test-pii-key")
        self.assertEqual(out["status"], "valid")
        assert_no_pii(self, json.dumps(out))


if __name__ == "__main__":
    unittest.main()
