import json
import re
import unittest
from datetime import datetime, timezone
from pathlib import Path

from lakeflow_core import maintenance
from lakeflow_core.backfill import MAX_KEYS, snapshot_signal
from lakeflow_core.contracts import load_registry
from lakeflow_core.quality import build_checks, evaluate, results_insert

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = load_registry(ROOT / "contracts")
ORDER_ID = "0b6d2f4e-8c1a-4d3b-9e7f-5a6b7c8d9e0f"


class FakeTrino:
    """Records statements and answers the metadata queries maintain_table issues."""

    def __init__(self, exists=True, files=((5, 3, 5000), (1, 0, 4000)), rewrite=(22, 11), digests=None, fail_on=None):
        self.sql: list[tuple[str, list | None]] = []
        self.exists, self.files, self.rewrite = exists, list(files), rewrite
        self.digests = digests or {22: (10, b"\x01"), 11: (10, b"\x01")}
        self.fail_on = fail_on

    def __call__(self, sql, params=None):
        self.sql.append((sql, params))
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError(f"boom in {self.fail_on}")
        if "information_schema.tables" in sql:
            return [{"n": 1 if self.exists else 0}]
        if '$files"' in sql:
            data, deletes, size = self.files.pop(0)
            return [{"data_files": data, "delete_files": deletes, "total_bytes": size}]
        if "count(*) AS snapshots" in sql:
            return [{"snapshots": 7, "last_commit": datetime(2026, 9, 25, tzinfo=timezone.utc)}]
        if "operation = 'replace'" in sql:
            return [] if self.rewrite is None else [{"snapshot_id": self.rewrite[0], "parent_id": self.rewrite[1]}]
        if "FOR VERSION AS OF" in sql:
            snapshot = int(re.search(r"FOR VERSION AS OF (\d+)", sql).group(1))
            rows, digest = self.digests[snapshot]
            return [{"row_count": rows, "digest": digest}]
        return []

    def executed(self, fragment):
        return [s for s, _ in self.sql if fragment in s]


class MaintenanceTests(unittest.TestCase):
    def test_statements_use_trino_table_procedures_with_validated_policy(self):
        sql = maintenance.statements("silver.orders", maintenance.Policy())
        self.assertEqual(
            sql["optimize"], "ALTER TABLE lakehouse.silver.\"orders\" EXECUTE optimize(file_size_threshold => '64MB')"
        )
        self.assertIn("expire_snapshots(retention_threshold => '1h')", sql["expire_snapshots"])
        self.assertIn("remove_orphan_files(retention_threshold => '1h')", sql["remove_orphan_files"])
        with self.assertRaises(ValueError):
            maintenance.Policy(snapshot_retention="1h'; DROP TABLE x --")
        with self.assertRaises(ValueError):
            maintenance.Policy(file_size_threshold="lots")

    def test_table_names_are_validated(self):
        self.assertEqual(maintenance.qualified("bronze.cdc_events", "$files"), 'lakehouse.bronze."cdc_events$files"')
        for bad in ("orders", "silver.orders; DROP", 'silver."x"', "a.b.c"):
            with self.assertRaises(ValueError):
                maintenance.qualified(bad)

    def test_default_tables_cover_bronze_silver_and_ops(self):
        silver = [REGISTRY.current(n).target_table for n in REGISTRY.names]
        tables = maintenance.default_tables(silver)
        self.assertEqual(tables[0], "bronze.cdc_events")
        self.assertIn("silver.orders", tables)
        self.assertIn("ops.batch_commits", tables)
        self.assertEqual(maintenance.fingerprint_expr("silver.orders"), maintenance._SILVER_FINGERPRINT)

    def test_successful_run_records_before_after_and_content_check(self):
        trino = FakeTrino()
        record = maintenance.maintain_table(trino, "silver.orders", runner="test", run_id="r1")
        self.assertEqual(record["status"], "succeeded", record)
        self.assertEqual((record["data_files_before"], record["data_files_after"]), (5, 1))
        self.assertEqual((record["delete_files_before"], record["delete_files_after"]), (3, 0))
        self.assertEqual(record["rewrite_snapshot_id"], 22)
        self.assertTrue(record["fingerprint_match"])
        order = [s.split(" EXECUTE ")[1].split("(")[0] for s in trino.executed(" EXECUTE ")]
        self.assertEqual(order, ["optimize", "expire_snapshots", "remove_orphan_files"])
        insert = trino.executed("INSERT INTO lakehouse.ops.maintenance_runs")
        self.assertEqual(len(insert), 1)
        params = next(p for s, p in trino.sql if s == insert[0])
        self.assertEqual(len(params), len(maintenance.RECORD_COLUMNS))
        self.assertEqual(params[maintenance.RECORD_COLUMNS.index("runner")], "test")

    def test_rewrite_that_changes_content_fails_loudly(self):
        trino = FakeTrino(digests={22: (9, b"\x02"), 11: (10, b"\x01")})
        record = maintenance.maintain_table(trino, "bronze.cdc_events")
        self.assertEqual(record["status"], "failed")
        self.assertFalse(record["fingerprint_match"])
        self.assertIn("logical content", record["error"])

    def test_missing_table_is_skipped_and_errors_are_recorded(self):
        skipped = maintenance.maintain_table(FakeTrino(exists=False), "ops.dlq_events")
        self.assertEqual(skipped["status"], "skipped")
        failed = maintenance.maintain_table(FakeTrino(fail_on="expire_snapshots"), "ops.dlq_events")
        self.assertEqual(failed["status"], "failed")
        self.assertIn("boom in expire_snapshots", failed["error"])

    def test_noop_optimize_has_no_rewrite_evidence(self):
        record = maintenance.maintain_table(FakeTrino(rewrite=None), "ops.batch_commits", tasks=["optimize"])
        self.assertEqual(record["status"], "succeeded")
        self.assertIsNone(record["rewrite_snapshot_id"])
        self.assertIsNone(record["fingerprint_match"])
        with self.assertRaises(ValueError):
            maintenance.maintain_table(FakeTrino(), "ops.batch_commits", tasks=["vacuum"])

    def test_commit_conflicts_are_retried_but_other_errors_are_not(self):
        calls = []

        def flaky(sql, params=None):
            calls.append(sql)
            if len(calls) < 3:
                raise RuntimeError("Failed to commit Iceberg update: CommitFailedException")
            return []

        maintenance._execute_with_retries(flaky, "ALTER TABLE t EXECUTE optimize", sleep=lambda _: None)
        self.assertEqual(len(calls), 3)

        def broken(sql, params=None):
            calls.append(sql)
            raise RuntimeError("line 1:1: mismatched input")

        calls.clear()
        with self.assertRaises(RuntimeError):
            maintenance._execute_with_retries(broken, "ALTER", sleep=lambda _: None)
        self.assertEqual(len(calls), 1)


class BackfillTests(unittest.TestCase):
    def test_key_filtered_signal(self):
        signal_id, kind, data = snapshot_signal(REGISTRY, "orders", keys=[ORDER_ID.upper()], signal_id="s1")
        self.assertEqual((signal_id, kind), ("s1", "execute-snapshot"))
        body = json.loads(data)
        self.assertEqual(body["type"], "incremental")
        self.assertEqual(body["data-collections"], ["shop.orders"])
        self.assertEqual(body["additional-conditions"][0]["filter"], f"order_id IN ('{ORDER_ID}')")

    def test_time_filtered_and_whole_table_signals(self):
        since = datetime(2026, 9, 25, 8, 0, tzinfo=timezone.utc)
        body = json.loads(snapshot_signal(REGISTRY, "customers", updated_since=since)[2])
        self.assertEqual(body["additional-conditions"][0]["filter"], "updated_at >= '2026-09-25T08:00:00+00:00'")
        self.assertNotIn("additional-conditions", json.loads(snapshot_signal(REGISTRY, "orders")[2]))

    def test_rejects_anything_that_is_not_a_validated_filter(self):
        cases = [
            {"contract_name": "payments"},
            {"contract_name": "orders", "keys": ["1' OR '1'='1"]},
            {"contract_name": "orders", "keys": [ORDER_ID] * (MAX_KEYS + 1)},
            {"contract_name": "orders", "updated_since": datetime(2026, 9, 25)},
        ]
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                snapshot_signal(REGISTRY, **case)


class QualityPersistenceTests(unittest.TestCase):
    def test_results_insert_binds_every_value(self):
        checks = build_checks(REGISTRY)[:3]
        results = [evaluate(c, 0) for c in checks]
        sql, params = results_insert("run1", "2026-09-25T00:00:00Z", "airflow", results)
        self.assertTrue(sql.startswith("INSERT INTO lakehouse.ops.quality_results VALUES (?"))
        self.assertEqual(sql.count("?"), len(params))
        self.assertEqual(len(params), 12 * len(results))
        self.assertEqual(params[11], "airflow")
        with self.assertRaises(ValueError):
            results_insert("run1", None, "api", [])


class ObservabilityConfigTests(unittest.TestCase):
    """Dashboards and alerts may only use metrics that the code actually exports (no decorative panels)."""

    @classmethod
    def setUpClass(cls):
        sources = [ROOT / "spark" / "lakeflow_stream" / "metrics.py", ROOT / "api" / "lakeflow_api" / "monitor.py"]
        cls.exported = set()
        for path in sources:
            cls.exported |= set(re.findall(r'"(lakeflow_[a-z0-9_]+)"', path.read_text(encoding="utf-8")))

    def used(self, text):
        return {re.sub(r"_(bucket|sum|count)$", "", n) for n in re.findall(r"\b(lakeflow_[a-z0-9_]+)\b", text)}

    def test_dashboard_queries_reference_exported_metrics(self):
        dashboard = json.loads((ROOT / "observability" / "grafana" / "dashboards" / "lakeflow.json").read_text("utf-8"))
        exprs = [t["expr"] for p in dashboard["panels"] for t in p.get("targets", [])]
        self.assertGreaterEqual(len(exprs), 12)
        unknown = self.used("\n".join(exprs)) - self.exported
        self.assertFalse(unknown, f"dashboard uses metrics nobody exports: {unknown}")
        ids = [p["id"] for p in dashboard["panels"]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(all(p["datasource"]["uid"] == "prometheus" for p in dashboard["panels"]))

    def test_alerts_reference_exported_metrics_and_existing_runbooks(self):
        text = (ROOT / "observability" / "prometheus" / "alerts.yml").read_text(encoding="utf-8")
        exprs = "\n".join(re.findall(r"expr: (.+)", text))
        unknown = self.used(exprs) - self.exported
        self.assertFalse(unknown, f"alerts use metrics nobody exports: {unknown}")
        for runbook in set(re.findall(r"runbook: (\S+)", text)):
            self.assertTrue((ROOT / runbook).exists(), f"missing runbook {runbook}")


class DagConsistencyTests(unittest.TestCase):
    def test_every_dag_file_is_listed_in_the_integrity_check(self):
        dags = ROOT / "airflow" / "dags"
        ids = set()
        for path in dags.glob("*.py"):
            ids |= set(re.findall(r'dag_id="([a-z_]+)"', path.read_text(encoding="utf-8")))
        check = (ROOT / "airflow" / "tests" / "check_dags.py").read_text(encoding="utf-8")
        expected = set(re.findall(r'^    "([a-z_]+)": ', check, flags=re.MULTILINE))
        self.assertEqual(ids, expected)
        ignored = (dags / ".airflowignore").read_text(encoding="utf-8").split()
        self.assertIn("lakeflow_dag_support.py", ignored)


if __name__ == "__main__":
    unittest.main()
