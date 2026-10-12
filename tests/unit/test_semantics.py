import random
import unittest

from lakeflow_core.semantics import ChangeEvent, apply_plan, plan_batch

LATENESS = 60_000


def ev(eid, pk, lsn, op="u", ts=None, offset=None, partition=0, topic="lakeflow.shop.orders", status="PAID"):
    return ChangeEvent(
        eid,
        "shop.orders",
        pk,
        op,
        lsn,
        ts if ts is not None else lsn * 1000,
        topic,
        partition,
        offset if offset is not None else lsn,
        {"order_id": pk, "status": status},
    )


class PlanTests(unittest.TestCase):
    def test_insert_update_delete_in_one_batch(self):
        silver = {}
        plan = plan_batch(
            0, [ev("a", "o1", 10, "c"), ev("b", "o1", 11), ev("c", "o1", 12, "d")], silver, {}, 0, LATENESS
        )
        self.assertEqual(plan.outcomes, {"a": "superseded", "b": "superseded", "c": "applied"})
        apply_plan(plan, silver)
        row = silver[("shop.orders", "o1")]
        self.assertTrue(row.is_deleted)
        self.assertEqual(row.source_lsn, 12)

    def test_duplicates_in_batch_and_across_batches(self):
        silver, bronze = {}, {}
        first = plan_batch(0, [ev("a", "o1", 10, "c"), ev("a", "o1", 10, "c", offset=99)], silver, bronze, 0, LATENESS)
        self.assertEqual(first.in_batch_duplicates, 1)
        apply_plan(first, silver)
        bronze.update({e: 0 for e in first.bronze_appends})
        second = plan_batch(1, [ev("a", "o1", 10, "c", offset=150)], silver, bronze, 0, LATENESS)
        self.assertEqual(second.prior_duplicates, {"a"})
        self.assertEqual(second.to_apply, [])
        self.assertEqual(second.bronze_appends, [])

    def test_out_of_order_event_is_stale_not_applied(self):
        silver = {}
        apply_plan(plan_batch(0, [ev("new", "o1", 20)], silver, {}, 0, LATENESS), silver)
        plan = plan_batch(1, [ev("old", "o1", 15, ts=1_000)], silver, {"new": 0}, 50_000, LATENESS)
        self.assertEqual(plan.outcomes["old"], "stale")
        self.assertIn("old", plan.late)
        apply_plan(plan, silver)
        self.assertEqual(silver[("shop.orders", "o1")].event_id, "new")

    def test_late_event_for_new_key_is_applied_and_flagged(self):
        plan = plan_batch(3, [ev("late", "o9", 5, "c", ts=1)], {}, {}, 10_000, LATENESS)
        self.assertEqual(plan.outcomes["late"], "applied")
        self.assertIn("late", plan.late)

    def test_delete_of_unseen_key_creates_tombstone_and_blocks_older_resurrection(self):
        silver = {}
        apply_plan(plan_batch(0, [ev("d", "o2", 30, "d")], silver, {}, 0, LATENESS), silver)
        self.assertTrue(silver[("shop.orders", "o2")].is_deleted)
        replayed_update = plan_batch(1, [ev("u", "o2", 25)], silver, {"d": 0}, 0, LATENESS)
        self.assertEqual(replayed_update.outcomes["u"], "stale")

    def test_reinsert_after_delete_resurrects(self):
        silver = {}
        apply_plan(plan_batch(0, [ev("d", "o3", 30, "d")], silver, {}, 0, LATENESS), silver)
        apply_plan(plan_batch(1, [ev("c", "o3", 31, "c")], silver, {"d": 0}, 0, LATENESS), silver)
        self.assertFalse(silver[("shop.orders", "o3")].is_deleted)

    def test_crash_after_commit_replay_is_idempotent_and_labels_are_stable(self):
        events = [ev("a", "o1", 10, "c"), ev("b", "o1", 11), ev("c", "o2", 12, "c")]
        silver = {}
        plan = plan_batch(7, events, silver, {}, 0, LATENESS)
        apply_plan(plan, silver)
        bronze = {e: 7 for e in plan.bronze_appends}
        snapshot = {k: vars(v).copy() for k, v in silver.items()}
        replay = plan_batch(7, events, silver, bronze, 0, LATENESS)
        self.assertEqual(replay.outcomes, plan.outcomes)
        self.assertEqual(replay.bronze_appends, [], "already appended by the crashed attempt")
        apply_plan(replay, silver)
        self.assertEqual({k: vars(v) for k, v in silver.items()}, snapshot)

    def test_watermark_is_monotonic(self):
        plan = plan_batch(0, [ev("a", "o1", 10, ts=100_000)], {}, {}, 90_000, LATENESS)
        self.assertEqual(plan.watermark_after_ms, 90_000)
        plan = plan_batch(1, [ev("b", "o1", 11, ts=200_000)], {}, {}, 90_000, LATENESS)
        self.assertEqual(plan.watermark_after_ms, 140_000)

    def test_random_sequences_converge_to_latest_lsn(self):
        rng = random.Random(42)
        for trial in range(50):
            silver, bronze = {}, {}
            events = [ev(f"e{trial}-{i}", f"o{rng.randrange(5)}", lsn=i + 1, op=rng.choice("cud")) for i in range(40)]
            deliveries = events + rng.sample(events, 10)
            rng.shuffle(deliveries)
            for batch_id in range(0, len(deliveries), 7):
                plan = plan_batch(batch_id, deliveries[batch_id : batch_id + 7], silver, bronze, 0, LATENESS)
                apply_plan(plan, silver)
                bronze.update({e: batch_id for e in plan.bronze_appends})
            for pk in {e.pk for e in events}:
                expected = max((e for e in events if e.pk == pk), key=lambda e: e.lsn)
                self.assertEqual(silver[("shop.orders", pk)].event_id, expected.event_id)
            self.assertEqual(set(bronze), {e.event_id for e in events})


def run_batches(batches, silver=None, bronze=None):
    """Plan and apply each batch in order, recording bronze like the sink. Returns (silver, bronze, plans)."""
    silver = {} if silver is None else silver
    bronze = {} if bronze is None else bronze
    plans = []
    for batch_id, events in batches:
        plan = plan_batch(batch_id, events, silver, bronze, 0, LATENESS)
        apply_plan(plan, silver)
        bronze.update({e: batch_id for e in plan.bronze_appends})
        plans.append(plan)
    return silver, bronze, plans


class DeleteAndReplayTests(unittest.TestCase):
    """Deletes are tombstones that keep their LSN, so nothing older can bring a row back."""

    def test_insert_update_delete_then_old_replay_across_batches_never_resurrects(self):
        insert, update, delete = ev("c", "o1", 10, "c"), ev("u", "o1", 11, status="SHIPPED"), ev("d", "o1", 12, "d")
        silver, bronze, _ = run_batches([(0, [insert]), (1, [update]), (2, [delete])])
        _, _, (redelivery,) = run_batches([(3, [update, insert])], silver, bronze)
        self.assertEqual(redelivery.prior_duplicates, {"c", "u"}, "exact redelivery is a duplicate everywhere")
        older_distinct_change = ev("u-old", "o1", 11, status="PENDING", offset=500)
        _, _, (late,) = run_batches([(4, [older_distinct_change])], silver, bronze)
        self.assertEqual(late.outcomes, {"u-old": "stale"})
        row = silver[("shop.orders", "o1")]
        self.assertTrue(row.is_deleted)
        self.assertEqual((row.event_id, row.source_lsn), ("d", 12))

    def test_legitimate_recreation_of_a_deleted_key_in_the_same_batch(self):
        events = [ev("c1", "o1", 10, "c"), ev("d", "o1", 11, "d"), ev("c2", "o1", 12, "c")]
        silver, _, (plan,) = run_batches([(0, events)])
        self.assertEqual(plan.outcomes, {"c1": "superseded", "d": "superseded", "c2": "applied"})
        self.assertFalse(silver[("shop.orders", "o1")].is_deleted)

    def test_repeated_delivery_of_a_delete_is_idempotent(self):
        delete = ev("d", "o1", 12, "d")
        silver, bronze, _ = run_batches([(0, [ev("c", "o1", 10, "c")]), (1, [delete, delete])])
        before = vars(silver[("shop.orders", "o1")]).copy()
        _, _, (again,) = run_batches([(2, [delete])], silver, bronze)
        self.assertEqual(again.to_apply, [])
        self.assertEqual(vars(silver[("shop.orders", "o1")]), before)


class OrderingTests(unittest.TestCase):
    def test_multiple_operations_for_one_key_converge_regardless_of_arrival_order(self):
        events = [ev("c", "o1", 10, "c"), ev("u1", "o1", 11), ev("u2", "o1", 12, status="SHIPPED")]
        events.append(ev("d", "o1", 13, "d"))
        expected = None
        for seed in range(10):
            shuffled = events[:]
            random.Random(seed).shuffle(shuffled)
            silver, _, (plan,) = run_batches([(0, shuffled)])
            outcome = (plan.outcomes, vars(silver[("shop.orders", "o1")]))
            expected = expected or outcome
            self.assertEqual(outcome, expected)
        self.assertEqual(expected[0]["d"], "applied")

    def test_equal_lsn_tie_for_one_key_is_broken_by_kafka_offset_deterministically(self):
        # Equal (table, pk, lsn) with different event ids: e.g. a snapshot read emitted at the position of the
        # last streamed change of that row. Both carry the same committed state; the later offset wins.
        stream = ev("stream", "o1", 20, "u", offset=5, status="PAID")
        snapshot_read = ev("snap", "o1", 20, "r", offset=9, status="PAID")
        for events in ([stream, snapshot_read], [snapshot_read, stream]):
            silver, _, (plan,) = run_batches([(0, events)])
            self.assertEqual(plan.outcomes, {"stream": "superseded", "snap": "applied"})
            self.assertEqual(silver[("shop.orders", "o1")].event_id, "snap")
        again = ev("again", "o1", 20, "u", offset=11)
        _, _, (next_batch,) = run_batches([(1, [again])], silver, {"stream": 0, "snap": 0})
        self.assertEqual(next_batch.outcomes, {"again": "stale"}, "equal LSN across batches never re-applies")

    def test_connector_restart_replays_a_suffix_without_changing_state(self):
        events = [ev(f"e{i}", f"o{i % 3}", 100 + i, "c" if i < 3 else "u", offset=i) for i in range(9)]
        silver, bronze, _ = run_batches([(0, events[:5]), (1, events[5:])])
        state = {k: vars(v).copy() for k, v in silver.items()}
        replayed = [ev(e.event_id, e.pk, e.lsn, e.op, offset=e.offset + 1000) for e in events[4:]]
        _, _, (plan,) = run_batches([(2, replayed)], silver, bronze)
        self.assertEqual(plan.prior_duplicates, {e.event_id for e in replayed})
        self.assertEqual(plan.bronze_appends, [])
        self.assertEqual({k: vars(v) for k, v in silver.items()}, state)

    def test_source_lsn_regression_is_unsupported_and_classified_stale(self):
        # Characterization, not a feature: if the source database is recreated (new cluster, restored volume)
        # while the lakehouse is kept, LSNs restart lower and every change to an existing key is "stale".
        # docs/runbooks/stream-recovery.md requires a lakehouse rebuild for that case.
        silver, bronze, _ = run_batches([(0, [ev("old-cluster", "o1", 9_000_000, "c")])])
        _, _, (plan,) = run_batches([(1, [ev("new-cluster", "o1", 50, "u", status="SHIPPED")])], silver, bronze)
        self.assertEqual(plan.outcomes, {"new-cluster": "stale"})
        self.assertEqual(silver[("shop.orders", "o1")].event_id, "old-cluster")


class SnapshotTests(unittest.TestCase):
    """Snapshot reads (op=r) use the same LSN guard: they restore missing rows but never overwrite newer state."""

    def test_snapshot_read_restores_a_row_missing_from_silver(self):
        silver, _, (plan,) = run_batches([(0, [ev("r1", "o1", 500, "r", status="SHIPPED")])])
        self.assertEqual(plan.outcomes, {"r1": "applied"})
        self.assertFalse(silver[("shop.orders", "o1")].is_deleted)

    def test_snapshot_read_older_than_silver_cannot_overwrite_a_newer_change(self):
        silver, bronze, _ = run_batches([(0, [ev("u", "o1", 600, status="DELIVERED")])])
        _, _, (plan,) = run_batches([(1, [ev("r-old", "o1", 550, "r", status="PENDING")])], silver, bronze)
        self.assertEqual(plan.outcomes, {"r-old": "stale"})
        self.assertEqual(silver[("shop.orders", "o1")].values["status"], "DELIVERED")

    def test_snapshot_read_cannot_resurrect_a_newer_delete(self):
        silver, bronze, _ = run_batches([(0, [ev("d", "o1", 700, "d")])])
        _, _, (plan,) = run_batches([(1, [ev("r-old", "o1", 650, "r")])], silver, bronze)
        self.assertEqual(plan.outcomes, {"r-old": "stale"})
        self.assertTrue(silver[("shop.orders", "o1")].is_deleted)

    def test_repeated_delivery_of_the_same_snapshot_read_is_a_duplicate(self):
        read = ev("r1", "o1", 500, "r")
        silver, bronze, _ = run_batches([(0, [read])])
        _, _, (plan,) = run_batches([(1, [ev("r1", "o1", 500, "r", offset=900)])], silver, bronze)
        self.assertEqual((plan.prior_duplicates, plan.to_apply), ({"r1"}, []))


class PartialCommitTests(unittest.TestCase):
    """The sink writes silver, then bronze, then DLQ, then the batch record; a crash can stop after any of them."""

    EVENTS = [ev("a", "o1", 10, "c"), ev("b", "o1", 11), ev("c", "o2", 12, "c"), ev("d", "o3", 13, "d")]

    def reference(self):
        silver, bronze, (plan,) = run_batches([(5, self.EVENTS)])
        return {k: vars(v).copy() for k, v in silver.items()}, dict(bronze), plan.outcomes

    def test_crash_after_silver_before_bronze_converges(self):
        expected_silver, expected_bronze, expected_outcomes = self.reference()
        silver = {}
        apply_plan(plan_batch(5, self.EVENTS, silver, {}, 0, LATENESS), silver)  # silver committed, bronze not
        replay = plan_batch(5, self.EVENTS, silver, {}, 0, LATENESS)
        self.assertEqual(replay.outcomes, expected_outcomes, "labels must not change on replay")
        self.assertEqual(sorted(replay.bronze_appends), sorted(expected_bronze), "bronze is completed on replay")
        apply_plan(replay, silver)
        self.assertEqual({k: vars(v) for k, v in silver.items()}, expected_silver)

    def test_crash_before_any_write_converges(self):
        expected_silver, expected_bronze, expected_outcomes = self.reference()
        silver, bronze, (plan,) = run_batches([(5, self.EVENTS)])
        self.assertEqual(plan.outcomes, expected_outcomes)
        self.assertEqual(({k: vars(v) for k, v in silver.items()}, bronze), (expected_silver, expected_bronze))

    def test_crash_after_every_write_replays_as_no_op(self):
        expected_silver, expected_bronze, expected_outcomes = self.reference()
        silver, bronze, _ = run_batches([(5, self.EVENTS)])
        _, _, (replay,) = run_batches([(5, self.EVENTS)], silver, bronze)
        self.assertEqual(replay.outcomes, expected_outcomes)
        self.assertEqual(replay.bronze_appends, [])
        self.assertEqual(({k: vars(v) for k, v in silver.items()}, bronze), (expected_silver, expected_bronze))


if __name__ == "__main__":
    unittest.main()
