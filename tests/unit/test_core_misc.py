import json
import unittest
from pathlib import Path

from lakeflow_core import benchmark, lineage, lsn, stats, tables
from lakeflow_core.adr import parse_adr
from lakeflow_core.contracts import load_registry
from lakeflow_core.quality import build_checks, evaluate

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = load_registry(ROOT / "contracts")


class StatsTests(unittest.TestCase):
    def test_percentiles_match_numpy_linear(self):
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.assertAlmostEqual(stats.percentile(values, 50), 5.5)
        self.assertAlmostEqual(stats.percentile(values, 95), 9.55)
        self.assertAlmostEqual(stats.percentile(values, 99), 9.91)
        self.assertIsNone(stats.percentile([], 50))

    def test_summary(self):
        summary = stats.summarize([10, 20, 30])
        self.assertEqual(summary["count"], 3)
        self.assertAlmostEqual(summary["mean"], 20)
        self.assertAlmostEqual(summary["stddev"], 10)
        self.assertEqual(stats.summarize([])["p95"], None)


class LsnTests(unittest.TestCase):
    def test_round_trip(self):
        self.assertEqual(lsn.lsn_to_int("16/B374D848"), 0x16B374D848)
        self.assertEqual(lsn.int_to_lsn(0x16B374D848), "16/B374D848")
        self.assertEqual(lsn.int_to_lsn(lsn.lsn_to_int("0/16C1C80")), "0/16C1C80")


class LineageTests(unittest.TestCase):
    def setUp(self):
        self.spec = lineage.load_spec(ROOT / "contracts" / "lineage.json")

    def test_impact_of_source_table(self):
        result = lineage.impact(self.spec, "postgres.shop.orders")
        self.assertIn("lakehouse.silver.orders", result["affected_datasets"])
        self.assertIn("spark.orders_cdc_stream", result["affected_jobs"])
        self.assertIn("analytics", result["owners_to_notify"])

    def test_upstream_of_silver(self):
        ids = [n["id"] for n in lineage.upstream(self.spec, "lakehouse.silver.orders")]
        self.assertIn("postgres.shop.orders", ids)

    def test_replay_feedback_loop_is_traversed_safely(self):
        affected = lineage.impact(self.spec, "lakehouse.ops.dlq_events")["affected_datasets"]
        self.assertIn("kafka.lakeflow.replay.cdc", affected)
        self.assertIn("lakehouse.silver.orders", affected)
        self.assertEqual(len(affected), len(set(affected)))

    def test_dangling_refs_rejected(self):
        dangling = json.loads(json.dumps(self.spec))
        dangling["jobs"][0]["outputs"].append("nowhere")
        with self.assertRaises(lineage.LineageError):
            lineage.validate_spec(dangling)


class AdrTests(unittest.TestCase):
    def test_parse_sections(self):
        text = "# ADR-0003: Delivery semantics\n\n**Status**: Accepted\n**Date**: 2026-09-25\n\n## Context\nA\n\n## Decision\nB\n"
        adr = parse_adr(text, "0003-x.md")
        self.assertEqual((adr["id"], adr["status"], adr["date"]), ("ADR-0003", "Accepted", "2026-09-25"))
        self.assertEqual(adr["sections"], {"Context": "A", "Decision": "B"})

    def test_repository_adrs_parse(self):
        adr_dir = ROOT / "docs" / "adr"
        paths = sorted(adr_dir.glob("[0-9][0-9][0-9][0-9]-*.md"))
        self.assertGreaterEqual(len(paths), 5)
        for path in paths:
            with self.subTest(path.name):
                adr = parse_adr(path.read_text(encoding="utf-8"), path.name)
                for section in ("Context", "Decision", "Consequences"):
                    self.assertIn(section, adr["sections"])


class QualityTests(unittest.TestCase):
    def test_checks_cover_every_contract(self):
        checks = {c.check_id: c for c in build_checks(REGISTRY)}
        for expected in (
            "orders.primary_key_unique",
            "orders.status_enum",
            "orders.customer_id_references_customers",
            "orders.source_reconciliation",
            "customers.source_reconciliation",
            "orders.freshness_p95",
            "bronze.event_id_unique",
            "dlq.open_records",
        ):
            self.assertIn(expected, checks)
        recon = checks["customers.source_reconciliation"].sql
        self.assertNotIn("email", recon, "PII columns must never be compared in plain SQL")
        self.assertIn("postgres.shop.orders", checks["orders.source_reconciliation"].sql)
        self.assertNotIn("channel", checks["orders.source_reconciliation"].sql, "v1 columns only")

    def test_evaluate(self):
        check = build_checks(REGISTRY)[0]
        self.assertEqual(evaluate(check, 0)["status"], "pass")
        self.assertEqual(evaluate(check, 3)["status"], "fail")
        self.assertEqual(evaluate(check, None)["status"], "no_data")
        self.assertEqual(evaluate(check, None, error="boom")["status"], "error")


class TablesTests(unittest.TestCase):
    def test_silver_columns_follow_contract_and_pii_policy(self):
        orders = dict(tables.silver_columns(REGISTRY, "orders"))
        self.assertEqual(orders["amount"], "decimal(12,2)")
        self.assertEqual(orders["channel"], "string")
        self.assertIn("_prev_source_lsn", orders)
        customers = dict(tables.silver_columns(REGISTRY, "customers"))
        self.assertIn("email_hmac", customers)
        self.assertNotIn("full_name", customers)


class BenchmarkTests(unittest.TestCase):
    def doc(self):
        iteration = {
            "events_expected": 100,
            "events_observed": 100,
            "duplicates_observed": 0,
            "dlq_events": 0,
            "reconciliation_mismatches": 0,
            "first_source_ms": 0,
            "last_commit_ms": 10_000,
            "latency_ms": [100, 200, 300, 400],
        }
        return {
            "schema_version": 1,
            "run_id": "r",
            "created_at": "2026-09-25T00:00:00Z",
            "git_sha": "abc",
            "environment": {},
            "config": {},
            "workload": {},
            "iterations": [iteration, dict(iteration)],
        }

    def test_summary_is_recomputed_from_raw_samples(self):
        summary = benchmark.summarize_result(self.doc())
        self.assertEqual(summary["events_observed"], 200)
        self.assertAlmostEqual(summary["throughput_eps"]["mean"], 10.0)
        self.assertAlmostEqual(summary["latency_ms"]["p50"], 250)
        self.assertEqual(summary["loss_rate"], 0)

    def test_thresholds(self):
        summary = benchmark.summarize_result(self.doc())
        checks = benchmark.evaluate_thresholds(summary, {"max_loss_rate": 0, "min_throughput_eps": 50})
        self.assertEqual([c["passed"] for c in checks], [True, False])
        with self.assertRaises(benchmark.BenchmarkResultError):
            benchmark.evaluate_thresholds(summary, {"max_unknown": 1})

    def test_invalid_result_rejected(self):
        with self.assertRaises(benchmark.BenchmarkResultError):
            benchmark.validate_result({"schema_version": 1})


if __name__ == "__main__":
    unittest.main()
