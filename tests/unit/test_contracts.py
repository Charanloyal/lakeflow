import copy
import json
import unittest
from pathlib import Path

from lakeflow_core.contracts import (
    ContractError,
    ContractRegistry,
    check_backward_compatible,
    contracts_fingerprint,
    load_registry,
    parse_contract,
)

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "contracts"
CORPUS = json.loads((CONTRACTS / "examples.json").read_text(encoding="utf-8"))


def case_record(case):
    record = copy.deepcopy(CORPUS["base"][case["contract"]])
    record.update(case.get("patch", {}))
    for name in case.get("remove", []):
        record.pop(name)
    return record


def load_doc(relative):
    return json.loads((CONTRACTS / relative).read_text(encoding="utf-8"))


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.registry = load_registry(CONTRACTS)

    def test_loads_active_contracts_only(self):
        self.assertEqual(self.registry.names, ["customers", "orders"])
        self.assertEqual([c.version for c in self.registry.versions("orders")], [1, 2])
        self.assertEqual(self.registry.current("orders").status, "current")
        self.assertEqual(self.registry.name_for_topic("lakeflow.shop.orders"), "orders")
        self.assertIsNone(self.registry.name_for_topic("lakeflow.shop.unknown"))

    def test_corpus_cases(self):
        with_proposed = load_registry(CONTRACTS, include_proposed=True)
        for case in CORPUS["cases"]:
            with self.subTest(case["name"]):
                result = self.registry.classify(case["contract"], case_record(case))
                expect = case["expect"]
                self.assertEqual(result.valid, expect["valid"])
                if expect["valid"]:
                    self.assertEqual(result.version, expect["version"])
                    self.assertEqual(list(result.drift_fields), expect.get("drift", []))
                else:
                    self.assertEqual([v.code for v in result.violations], expect["violations"])
                if "valid_with_proposed" in expect:
                    promoted = with_proposed.classify(case["contract"], case_record(case))
                    self.assertEqual(promoted.valid, expect["valid_with_proposed"])
                    self.assertEqual(promoted.version, expect["version_with_proposed"])

    def test_every_version_accepts_all_older_valid_rows(self):
        """BACKWARD_TRANSITIVE: nothing valid under an older version may become invalid."""
        base = CORPUS["base"]["orders"]
        for contract in self.registry.versions("orders"):
            self.assertEqual(contract.validate(base), [], f"v{contract.version} rejects the v1 base row")

    def test_fingerprint_matches_and_changes(self):
        self.assertEqual(self.registry.fingerprint, contracts_fingerprint(CONTRACTS))

    def test_proposed_version_is_promoted_only_on_request(self):
        promoted = load_registry(CONTRACTS, include_proposed=True)
        self.assertEqual(promoted.current("orders").version, 3)
        self.assertEqual([c.status for c in promoted.versions("orders")], ["superseded", "superseded", "current"])


class CompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.v1 = parse_contract(load_doc("orders/v1.schema.json"))
        self.v2_doc = load_doc("orders/v2.schema.json")

    def test_v1_to_v2_is_backward_compatible(self):
        self.assertEqual(check_backward_compatible(self.v1, parse_contract(self.v2_doc)), [])

    def test_new_required_field_breaks(self):
        doc = copy.deepcopy(self.v2_doc)
        doc["required"].append("channel")
        doc["properties"]["channel"]["type"] = "string"
        doc["properties"]["channel"]["enum"] = ["web", "mobile", "store"]
        problems = check_backward_compatible(self.v1, parse_contract(doc))
        self.assertIn("'channel' became required", problems)

    def test_enum_narrowing_breaks(self):
        doc = copy.deepcopy(self.v2_doc)
        doc["properties"]["currency"]["enum"] = ["USD"]
        self.assertIn("'currency' enum narrowed", check_backward_compatible(self.v1, parse_contract(doc)))

    def test_type_change_and_pk_change_break(self):
        doc = copy.deepcopy(self.v2_doc)
        doc["properties"]["amount"] = {"type": "number"}
        doc["x-lakeflow"]["primary_key"] = ["customer_id"]
        problems = check_backward_compatible(self.v1, parse_contract(doc))
        self.assertTrue(any("amount" in p for p in problems))
        self.assertTrue(any("primary key" in p for p in problems))

    def test_registry_rejects_incompatible_evolution(self):
        doc = copy.deepcopy(self.v2_doc)
        doc["properties"]["status"]["enum"] = ["PENDING"]
        with self.assertRaises(ContractError):
            ContractRegistry([self.v1, parse_contract(doc)])


class ContractParsingTests(unittest.TestCase):
    def test_unsupported_keyword_fails_fast(self):
        doc = load_doc("orders/v2.schema.json")
        doc["properties"]["amount"]["minimum"] = 0
        with self.assertRaises(ContractError):
            parse_contract(doc)

    def test_direct_pii_must_be_protected(self):
        doc = load_doc("customers/v1.schema.json")
        doc["properties"]["email"]["x-pii-handling"] = "keep"
        with self.assertRaises(ContractError):
            parse_contract(doc)

    def test_pii_target_columns(self):
        customers = parse_contract(load_doc("customers/v1.schema.json"))
        self.assertEqual(customers.fields["email"].target_column, "email_hmac")
        self.assertIsNone(customers.fields["full_name"].target_column)
        self.assertEqual(customers.fields["created_at"].logical_type, "timestamp")

    def test_logical_types(self):
        orders = parse_contract(load_doc("orders/v2.schema.json"))
        self.assertEqual(orders.fields["amount"].logical_type, "decimal(12,2)")
        self.assertTrue(orders.fields["channel"].nullable)
        self.assertEqual(orders.references[0]["contract"], "customers")


if __name__ == "__main__":
    unittest.main()
