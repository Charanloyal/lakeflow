import json
import unittest
from pathlib import Path

from lakeflow_core.contracts import load_registry
from lakeflow_core.decode import DECODED_SCHEMA, decode_change_event
from lakeflow_core.identity import pii_hmac

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = load_registry(ROOT / "contracts")
PII_KEY = b"unit-test-pii-key"
ORDER = {
    "order_id": "3f2b8f7e-5d1a-4c9b-9f60-1a2b3c4d5e6f",
    "customer_id": "0b7e7c1e-2f7a-4c55-8f0e-6a1d2c3b4a59",
    "status": "PENDING",
    "amount": "129.90",
    "currency": "USD",
    "created_at": "2026-09-25T10:00:00.1234Z",
    "updated_at": "2026-09-25T12:00:00+02:00",
}
TOPIC = "lakeflow.shop.orders"


def envelope(op="c", before=None, after=None, lsn=24023184, tx=747, ts=1790323163123, dbz_ts=1790323163456, **src):
    source = {
        "version": "2.7.3.Final",
        "connector": "postgresql",
        "name": "lakeflow",
        "ts_ms": ts,
        "snapshot": "false",
        "db": "lakeflow",
        "schema": "shop",
        "table": "orders",
        "txId": tx,
        "lsn": lsn,
    }
    source.update(src)
    doc = {"before": before, "after": after, "source": source, "op": op, "ts_ms": dbz_ts, "transaction": None}
    return json.dumps(doc).encode()


def key(order_id=ORDER["order_id"]):
    return json.dumps({"order_id": order_id}).encode()


def decode(value, k=None, topic=TOPIC):
    return decode_change_event(topic, key() if k is None else k, value, REGISTRY, PII_KEY)


class DecodeTests(unittest.TestCase):
    def test_output_has_exactly_the_declared_columns(self):
        self.assertEqual(set(decode(envelope(after=ORDER))), {name for name, _ in DECODED_SCHEMA})

    def test_valid_insert_is_normalized(self):
        out = decode(envelope(after=ORDER))
        self.assertEqual(out["status"], "valid")
        self.assertEqual(
            (out["op"], out["primary_key"], out["source_lsn"], out["contract_version"]),
            ("c", ORDER["order_id"], 24023184, 1),
        )
        after = json.loads(out["after_json"])
        self.assertEqual(after["created_at"], "2026-09-25T10:00:00.123400Z")
        self.assertEqual(after["updated_at"], "2026-09-25T10:00:00.000000Z")
        self.assertIsNone(out["before_json"])
        self.assertIsNone(out["raw_value"], "raw payload is only kept for DLQ records")

    def test_tombstone(self):
        self.assertEqual(decode(None)["status"], "tombstone")

    def test_event_id_is_stable_across_redelivery(self):
        first = decode(envelope(after=ORDER, dbz_ts=1))
        redelivered = decode(envelope(after=ORDER, dbz_ts=999999))
        self.assertEqual(first["event_id"], redelivered["event_id"])
        different_lsn = decode(envelope(after=ORDER, lsn=24023999))
        self.assertNotEqual(first["event_id"], different_lsn["event_id"])

    def test_delete_uses_before_image(self):
        out = decode(envelope(op="d", before=ORDER, after=None))
        self.assertEqual((out["status"], out["op"]), ("valid", "d"))
        self.assertIsNotNone(out["before_json"])

    def test_dlq_reasons(self):
        cases = {
            "MALFORMED_JSON": decode(b"{not json"),
            "MALFORMED_ENCODING": decode(b"\xff\xfe\xfd"),
            "INVALID_ENVELOPE": decode(b"[1, 2]"),
            "UNSUPPORTED_OP": decode(envelope(op="t")),
            "MISSING_ROW_IMAGE": decode(envelope(op="c", after=None)),
            "CONTRACT_VIOLATION": decode(envelope(after={**ORDER, "amount": "-5.00"})),
            "KEY_MISMATCH": decode(envelope(after=ORDER), k=key("00000000-0000-0000-0000-000000000000")),
            "UNKNOWN_TOPIC": decode(envelope(after=ORDER), topic="lakeflow.shop.unknown"),
        }
        for code, out in cases.items():
            with self.subTest(code):
                self.assertEqual(out["status"], "dlq")
                self.assertEqual(out["error_code"], code)
                self.assertIsNotNone(out["raw_value"])
        self.assertEqual(cases["CONTRACT_VIOLATION"]["violations"], ["amount.pattern"])
        self.assertTrue(cases["MALFORMED_ENCODING"]["raw_value"].startswith("base64:"))

    def test_envelope_integrity_checks(self):
        self.assertEqual(decode(envelope(after=ORDER, lsn="24023184"))["error_code"], "INVALID_ENVELOPE")
        self.assertEqual(decode(envelope(after=ORDER, table="customers"))["error_code"], "INVALID_ENVELOPE")
        self.assertEqual(decode(envelope(after=ORDER, lsn=True))["error_code"], "INVALID_ENVELOPE")

    def test_drift_fields_are_reported_but_not_stored(self):
        out = decode(envelope(after={**ORDER, "internal_note": "call me at 555-0100"}))
        self.assertEqual(out["status"], "valid")
        self.assertEqual(out["drift_fields"], ["internal_note"])
        self.assertNotIn("internal_note", json.loads(out["after_json"]))

    def test_customer_pii_is_hashed_and_dropped(self):
        customer = {
            "customer_id": ORDER["customer_id"],
            "email": "Ada.Lovelace@Example.com",
            "full_name": "Ada",
            "country": "GB",
            "created_at": "2026-09-01T08:30:00Z",
            "updated_at": "2026-09-01T08:30:00Z",
        }
        value = envelope(after=customer, table="customers")
        out = decode_change_event(
            "lakeflow.shop.customers",
            json.dumps({"customer_id": customer["customer_id"]}).encode(),
            value,
            REGISTRY,
            PII_KEY,
        )
        self.assertEqual(out["status"], "valid", out["error_detail"])
        after = json.loads(out["after_json"])
        self.assertEqual(after["email_hmac"], pii_hmac("ada.lovelace@example.com", PII_KEY))
        self.assertNotIn("full_name", after)
        self.assertNotIn("email", after)

    def test_decoder_never_raises(self):
        out = decode(envelope(op="u", before={**ORDER, "created_at": "garbage"}, after=ORDER))
        self.assertEqual(out["status"], "dlq")
        self.assertEqual(out["error_code"], "MALFORMED_ROW_IMAGE")


if __name__ == "__main__":
    unittest.main()
