"""The recorded-demo gate (scripts/check_recorded_demo.py) on synthetic snapshots: one defect per case."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("check_recorded_demo", ROOT / "scripts" / "check_recorded_demo.py")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)

META = {
    "sha": "29847ea8ea034b4908cb232cecc6d3e4763331cf",
    "captured_at": "2026-10-09T12:00:00Z",
    "run_url": "https://github.com/example-owner/lakeflow/actions/runs/123456",
    "repo_url": "https://github.com/example-owner/lakeflow",
    "codespaces_url": "https://codespaces.new/example-owner/lakeflow?quickstart=1",
    "mode": "recorded",
}
SECRET = "generated-secret-value-0123456789"


class RecordedDemoGateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.snapshot = self.dir / "snapshot"
        self.snapshot.mkdir()
        self.env = self.dir / ".env"
        self.env.write_text(f"LAKEFLOW_SESSION_SECRET={SECRET}\nLAKEFLOW_ADMIN_USER=admin\n", encoding="utf-8")
        self.write("meta.json", META)
        self.write("manifest.json", {"/api/metrics/overview": "r0000.json", "/api/quality/schema": "r0001.json"})
        self.write("r0000.json", {"overall": "healthy", "metrics": [], "as_of": "2026-10-09T11:59:00Z"})
        self.write(
            "r0001.json",
            {"contracts": [{"owner": {"contact": "identity-data@example.com"}, "fields": [{"name": "email"}]}]},
        )

    def write(self, name, doc):
        text = doc if isinstance(doc, str) else json.dumps(doc)
        (self.snapshot / name).write_text(text, encoding="utf-8")

    def problems(self):
        return gate.check(self.snapshot, self.env, ROOT / "contracts")

    def assert_problem(self, fragment):
        problems = self.problems()
        self.assertTrue(any(fragment in p for p in problems), f"expected {fragment!r} in {problems}")

    def test_clean_synthetic_snapshot_passes(self):
        self.assertEqual(self.problems(), [])

    def test_missing_or_malformed_provenance_fails(self):
        (self.snapshot / "meta.json").unlink()
        self.assert_problem("meta.json is missing")
        self.write("meta.json", "{broken")
        self.assert_problem("not valid JSON")
        for key, value, fragment in [
            ("sha", "abc", "sha"),
            ("captured_at", "yesterday", "captured_at"),
            ("captured_at", "2026-10-09T12:00:00", "timezone"),
            ("run_url", "https://example.com/run", "run_url"),
            ("codespaces_url", "javascript:alert(1)", "codespaces_url"),
            ("mode", "live", "mode"),
        ]:
            with self.subTest(key=key, value=value):
                self.write("meta.json", {**META, key: value})
                self.assert_problem(fragment)

    def test_manifest_must_reference_existing_get_responses(self):
        self.write("manifest.json", {"/api/metrics/overview": "r0099.json"})
        self.assert_problem("missing file r0099.json")
        self.write("manifest.json", {"/api/auth/login": "r0000.json"})
        self.assert_problem("must never be part of a recording")
        self.write("manifest.json", {"/etc/passwd": "r0000.json"})
        self.assert_problem("not an API path")
        self.write("manifest.json", {"/api/metrics/overview": "../meta.json"})
        self.assert_problem("unexpected file name")
        (self.snapshot / "manifest.json").unlink()
        self.assert_problem("manifest.json is missing")

    def test_secret_from_env_file_is_detected_without_printing_it(self):
        self.write("r0000.json", {"detail": f"connection string with {SECRET}"})
        problems = self.problems()
        self.assertTrue(any("LAKEFLOW_SESSION_SECRET" in p for p in problems), problems)
        self.assertFalse(any(SECRET in p for p in problems), "the gate must never echo the secret")

    def test_well_known_secret_patterns_are_detected(self):
        header = "-----BEGIN " + "PRIVATE KEY-----"  # split so repository secret scans do not flag this test
        self.write("r0000.json", {"note": header})
        self.assert_problem("private key")

    def test_real_looking_email_is_detected(self):
        self.write("r0000.json", {"source_state": {"contact": "someone@realcompany.co"}})
        self.assert_problem("non-reserved domain 'realcompany.co'")

    def test_reserved_domains_are_allowed(self):
        self.write("r0000.json", {"a": "customer1@example.com", "b": "bench-x@example.invalid", "c": "x@test"})
        self.assertEqual(self.problems(), [])

    def test_unredacted_direct_pii_fields_are_detected_even_inside_raw_json_strings(self):
        self.write("r0000.json", {"source_state": {"customer_id": "c1", "full_name": "Synthetic Person"}})
        self.assert_problem("unredacted direct-PII field 'full_name'")
        raw = json.dumps({"after": {"email": "masked-but-not-marked"}})
        self.write("r0000.json", {"records": [{"raw_value": raw}]})
        self.assert_problem("unredacted direct-PII field 'email'")
        masked = json.dumps({"after": {"email": "[redacted: direct PII]", "full_name": "[redacted: direct PII]"}})
        self.write("r0000.json", {"records": [{"raw_value": masked}, {"raw_value": "[withheld: binary]"}]})
        self.assertEqual(self.problems(), [])

    def test_cli_exit_code(self):
        self.assertEqual(gate.main([str(self.snapshot), "--env", str(self.env)]), 0)
        self.write("meta.json", {**META, "mode": "live"})
        self.assertEqual(gate.main([str(self.snapshot), "--env", str(self.env)]), 1)


if __name__ == "__main__":
    unittest.main()
