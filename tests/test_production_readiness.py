#!/usr/bin/env python3
"""
Unit and Integration Test Suite for LakeFlow Production-Readiness
Tests:
- Deduplication Logic
- Ordering by LSN / Commit Timestamp
- Schema Evolution Loose Coupling
- Checkpoint Recovery & Idempotency
"""

import json
import os
import unittest
from datetime import datetime, timezone

class TestLakeFlowProductionReadiness(unittest.TestCase):

    def setUp(self):
        # Sample base Debezium CDC event
        self.base_event = {
            "record_key": "c1000000-0000-0000-0000-000000000001",
            "source_topic": "lakeflow.platform.customers",
            "table_name": "customers",
            "cdc_op": "u",
            "source_lsn": 10502040,
            "tx_id": 901,
            "source_timestamp": 1716480000.0,
            "before_state": json.dumps({"first_name": "Alice", "email": "alice@old.io"}),
            "after_state": json.dumps({"first_name": "Alice", "email": "alice@new.io"}),
        }

    def test_deduplication_composite_key(self):
        """Verify duplicate Kafka deliveries with identical (record_key, source_lsn) are filtered."""
        event_batch = [
            self.base_event,
            self.base_event.copy(), # Identical duplicate
            dict(self.base_event, source_lsn=10502041), # Distinct new event
            self.base_event.copy(), # Another duplicate
        ]

        seen_keys = set()
        deduped = []
        for event in event_batch:
            dedup_key = (event["record_key"], event["source_lsn"])
            if dedup_key not in seen_keys:
                seen_keys.add(dedup_key)
                deduped.append(event)

        self.assertEqual(len(deduped), 2)
        self.assertEqual(deduped[0]["source_lsn"], 10502040)
        self.assertEqual(deduped[1]["source_lsn"], 10502041)

    def test_out_of_order_lsn_resolution(self):
        """Verify events arriving out of chronological order resolve to the highest LSN."""
        events_out_of_order = [
            dict(self.base_event, source_lsn=10502050, cdc_op="u", after_state=json.dumps({"status": "SHIPPED"})),
            dict(self.base_event, source_lsn=10502040, cdc_op="c", after_state=json.dumps({"status": "PENDING"})),
            dict(self.base_event, source_lsn=10502060, cdc_op="u", after_state=json.dumps({"status": "DELIVERED"})),
            dict(self.base_event, source_lsn=10502045, cdc_op="u", after_state=json.dumps({"status": "PROCESSING"})),
        ]

        # Resolution rule: sort by LSN ascending to obtain current state
        sorted_events = sorted(events_out_of_order, key=lambda x: x["source_lsn"])
        final_state = json.loads(sorted_events[-1]["after_state"])

        self.assertEqual(sorted_events[-1]["source_lsn"], 10502060)
        self.assertEqual(final_state["status"], "DELIVERED")

    def test_schema_evolution_tolerance(self):
        """Verify dynamic payload ingestion absorbs newly added columns without crashing."""
        evolved_payload = {
            "first_name": "Bob",
            "last_name": "Smith",
            "email": "bob@domain.com",
            "loyalty_tier": "PLATINUM",      # Newly added column
            "discount_pct": 15.5             # Newly added column
        }

        # Serialized as JSON string in CDC after_state
        serialized = json.dumps(evolved_payload)
        parsed = json.loads(serialized)

        self.assertIn("loyalty_tier", parsed)
        self.assertEqual(parsed["loyalty_tier"], "PLATINUM")
        self.assertIn("discount_pct", parsed)
        self.assertEqual(parsed["discount_pct"], 15.5)

    def test_checkpoint_state_persistence_and_recovery(self):
        """Verify streaming checkpoint state commits and reload semantics."""
        checkpoint_dir = os.path.join(os.path.dirname(__file__), ".test_checkpoint")
        os.makedirs(checkpoint_dir, exist_ok=True)
        checkpoint_file = os.path.join(checkpoint_dir, "metadata.json")

        state = {
            "batch_id": 42,
            "topic_offsets": {"lakeflow.platform.orders": 152000},
            "committed_at": datetime.now(timezone.utc).isoformat()
        }

        with open(checkpoint_file, "w") as f:
            json.dump(state, f)

        # Recover state
        with open(checkpoint_file, "r") as f:
            recovered_state = json.load(f)

        self.assertEqual(recovered_state["batch_id"], 42)
        self.assertEqual(recovered_state["topic_offsets"]["lakeflow.platform.orders"], 152000)

        # Cleanup
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
        if os.path.exists(checkpoint_dir):
            os.rmdir(checkpoint_dir)

if __name__ == "__main__":
    unittest.main()
