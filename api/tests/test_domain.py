"""Control-plane domain logic (stdlib only: runs without FastAPI or any service)."""

import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from lakeflow_api.domain import promtext
from lakeflow_api.domain.checkpoint import consumer_lag, parse_offsets_file, read_checkpoint
from lakeflow_api.domain.ratelimit import RateLimiter
from lakeflow_api.domain.sessions import Identity, check_credentials, parse_basic, sign_session, verify_session
from lakeflow_api.domain.trace import assemble

SECRET = "unit-test-secret-0123456789"
OFFSETS = 'v1\n{"batchWatermarkMs":0,"batchTimestampMs":1790000000000,"conf":{}}\n{"lakeflow.shop.orders":{"0":12,"1":7,"2":9}}\n'


class SessionTests(unittest.TestCase):
    def test_round_trip_and_tamper(self):
        token = sign_session(Identity("admin", "admin"), SECRET)
        self.assertEqual(verify_session(token, SECRET), Identity("admin", "admin"))
        body, mac = token.split(".")
        forged = sign_session(Identity("admin", "admin"), "other-secret-000000000").split(".")[1]
        self.assertIsNone(verify_session(f"{body}.{forged}", SECRET))
        self.assertIsNone(verify_session("garbage", SECRET))

    def test_expiry(self):
        token = sign_session(Identity("viewer", "viewer"), SECRET, ttl_s=10, now=1000)
        self.assertIsNotNone(verify_session(token, SECRET, now=1005))
        self.assertIsNone(verify_session(token, SECRET, now=1011))

    def test_credentials(self):
        users = {"admin": ("pw", "admin"), "viewer": ("vpw", "viewer")}
        self.assertEqual(check_credentials("admin", "pw", users).role, "admin")
        self.assertIsNone(check_credentials("admin", "wrong", users))
        self.assertIsNone(check_credentials("nobody", "pw", users))

    def test_parse_basic(self):
        self.assertEqual(parse_basic("Basic YWRtaW46cGE6c3M="), ("admin", "pa:ss"))
        self.assertIsNone(parse_basic("Bearer x"))
        self.assertIsNone(parse_basic("Basic !!!"))


class RateLimitTests(unittest.TestCase):
    def test_sliding_window(self):
        now = [0.0]
        limiter = RateLimiter(2, 10, clock=lambda: now[0])
        self.assertTrue(limiter.allow("u")[0])
        self.assertTrue(limiter.allow("u")[0])
        allowed, retry = limiter.allow("u")
        self.assertFalse(allowed)
        self.assertAlmostEqual(retry, 10)
        now[0] = 10.5
        self.assertTrue(limiter.allow("u")[0])
        self.assertTrue(limiter.allow("other")[0])


class CheckpointTests(unittest.TestCase):
    def test_reads_offsets_commits_and_epoch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "query" / "offsets").mkdir(parents=True)
            (root / "query" / "commits").mkdir(parents=True)
            (root / "stream-epoch").write_text("abc123")
            for n in (0, 1, 2):
                (root / "query" / "offsets" / str(n)).write_text(OFFSETS)
            for n in (0, 1):
                (root / "query" / "commits" / str(n)).write_text('v1\n{"nextBatchWatermarkMs":0}')
            state = read_checkpoint(root)
            self.assertEqual((state.epoch, state.latest_offsets_batch, state.latest_committed_batch), ("abc123", 2, 1))
            self.assertTrue(state.in_flight)
            self.assertTrue(state.is_committed(1))
            self.assertFalse(state.is_committed(2))
            self.assertEqual(state.committed_offsets["lakeflow.shop.orders"]["0"], 12)

    def test_lag(self):
        committed = parse_offsets_file(OFFSETS)
        end = {"lakeflow.shop.orders": {"0": 15, "1": 7, "2": 20}}
        self.assertEqual(consumer_lag(end, committed), {"lakeflow.shop.orders": 14})

    def test_missing_checkpoint_is_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = read_checkpoint(tmp)
            self.assertIsNone(state.latest_committed_batch)
            self.assertFalse(state.in_flight)


class PromTextTests(unittest.TestCase):
    def test_parse(self):
        text = (
            "# HELP lakeflow_stream_query_active 1 while running\n"
            "lakeflow_stream_query_active 1.0\n"
            'lakeflow_stream_events_total{table="shop.orders",outcome="applied"} 42.0\n'
            'lakeflow_stream_events_total{table="shop.orders",outcome="stale"} 3.0\n'
        )
        samples = promtext.parse(text)
        self.assertEqual(promtext.value(samples, "lakeflow_stream_query_active"), 1.0)
        self.assertEqual(promtext.total(samples, "lakeflow_stream_events_total", table="shop.orders"), 45.0)
        self.assertIsNone(promtext.value(samples, "missing"))


def bronze(lsn, outcome="applied", batch=3):
    return {"event_id": f"e{lsn}", "op": "u", "source_lsn": lsn, "source_tx_id": 700, "source_ts": "2026-09-25T10:00:00+00:00",
            "source_snapshot": "false", "debezium_ts": "2026-09-25T10:00:00.500000+00:00", "kafka_topic": "lakeflow.shop.orders",
            "kafka_partition": 1, "kafka_offset": 40 + lsn, "kafka_ts": "2026-09-25T10:00:00.600000+00:00",
            "contract_version": 1, "is_late": False, "apply_outcome": outcome, "injection_id": None, "replay_of": None,
            "batch_id": batch, "stream_epoch": "ep", "committed_at": "2026-09-25T10:00:07+00:00", "before": None, "after": {}}


class TraceTests(unittest.TestCase):
    def base(self, **kw):
        args = dict(table="orders", key="k1", mutation=None, source_row={"order_id": "k1"}, kafka_events=[], bronze_rows=[],
                    dlq_rows=[], batches={}, committed_batch={}, silver_row=None, trino_ms=12.0,
                    now=datetime(2026, 9, 25, 10, 0, 9, tzinfo=timezone.utc))
        args.update(kw)
        return assemble(**args)

    def stages(self, trace):
        return {s["stage"]: s["status"] for s in trace["stages"]}

    def test_nothing_downstream_yet(self):
        trace = self.base()
        self.assertEqual(self.stages(trace), {"postgres": "done", "debezium": "pending", "kafka": "pending", "spark": "pending",
                                              "checkpoint": "pending", "iceberg": "pending", "trino": "pending"})

    def test_fully_traced_event(self):
        batches = {("ep", 3): {"attempts": 1, "committed_at": "2026-09-25T10:00:07+00:00",
                               "snapshot_ids": {"silver.orders": 8123}}}
        trace = self.base(bronze_rows=[bronze(100)], batches=batches, committed_batch={"ep": 3},
                          silver_row={"_source_lsn": 100, "is_deleted": False})
        self.assertTrue(all(s == "done" for s in self.stages(trace).values()), trace["stages"])
        iceberg = next(s for s in trace["stages"] if s["stage"] == "iceberg")
        self.assertEqual(iceberg["details"]["snapshot_id"], 8123)
        self.assertEqual(trace["freshness_ms"], 7000)
        self.assertEqual(trace["events"][0]["lsn"], "0/64")

    def test_checkpoint_pending_between_iceberg_and_commit_marker(self):
        batches = {("ep", 3): {"attempts": 1, "committed_at": "2026-09-25T10:00:07+00:00", "snapshot_ids": {"silver.orders": 1}}}
        trace = self.base(bronze_rows=[bronze(100)], batches=batches, committed_batch={"ep": 2},
                          silver_row={"_source_lsn": 100})
        self.assertEqual(self.stages(trace)["checkpoint"], "pending")

    def test_stale_event_skips_iceberg(self):
        trace = self.base(bronze_rows=[bronze(90, outcome="stale")], silver_row={"_source_lsn": 100}, committed_batch={"ep": 3})
        self.assertEqual(self.stages(trace)["iceberg"], "skipped")
        self.assertEqual(self.stages(trace)["trino"], "done")
        self.assertIsNone(trace["freshness_ms"])

    def test_rejected_event(self):
        dlq = [{"dlq_id": "d" * 64, "error_code": "CONTRACT_VIOLATION", "kafka_topic": "lakeflow.shop.orders",
                "kafka_partition": 0, "kafka_offset": 5, "kafka_ts": None, "first_seen_at": "2026-09-25T10:00:01+00:00"}]
        trace = self.base(dlq_rows=dlq)
        self.assertEqual(self.stages(trace)["spark"], "failed")

    def test_live_kafka_event_before_bronze(self):
        live = [{"topic": "lakeflow.shop.orders", "partition": 2, "offset": 77, "kafka_ts_ms": int(time.time() * 1000),
                 "lsn": 555, "tx_id": 9, "op": "c", "source_ts_ms": 1790000000000, "debezium_ts_ms": 1790000000100,
                 "snapshot": "false", "injection_id": None}]
        trace = self.base(kafka_events=live)
        stages = self.stages(trace)
        self.assertEqual((stages["debezium"], stages["kafka"], stages["spark"]), ("done", "done", "pending"))
        kafka = next(s for s in trace["stages"] if s["stage"] == "kafka")
        self.assertEqual((kafka["details"]["partition"], kafka["details"]["offset"]), (2, 77))


if __name__ == "__main__":
    unittest.main()
