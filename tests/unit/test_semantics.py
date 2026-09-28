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


if __name__ == "__main__":
    unittest.main()
