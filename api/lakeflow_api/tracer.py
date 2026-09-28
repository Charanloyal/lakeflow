"""Background Kafka tail: indexes recent CDC events and batch commits, feeds SSE subscribers.

It reads from the log end at startup with its own consumer (assign mode, no group commits), so it never interferes
with Spark. History before the API started is served from bronze through Trino.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from collections import OrderedDict, deque

from confluent_kafka import OFFSET_END, Consumer, TopicPartition

log = logging.getLogger("lakeflow.tracer")


class Broadcaster:
    def __init__(self, max_subscribers: int = 50):
        self._subscribers: set[queue.Queue] = set()
        self._lock = threading.Lock()
        self.max_subscribers = max_subscribers

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=500)
        with self._lock:
            if len(self._subscribers) >= self.max_subscribers:
                raise RuntimeError("too many live subscribers")
            self._subscribers.add(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._lock:
            self._subscribers.discard(q)

    def publish(self, kind: str, payload: dict) -> None:
        with self._lock:
            subscribers = list(self._subscribers)
        for q in subscribers:
            try:
                q.put_nowait((kind, payload))
            except queue.Full:
                pass  # slow client: drop, the UI re-polls REST endpoints


class Tracer:
    def __init__(self, bootstrap: str, cdc_topics: tuple[str, ...], ops_topic: str, heartbeat_topic: str,
                 broadcaster: Broadcaster, max_events: int = 20000):
        self.bootstrap = bootstrap
        self.topics = tuple(cdc_topics) + (ops_topic, heartbeat_topic)
        self.cdc_topics = set(cdc_topics)
        self.ops_topic, self.heartbeat_topic = ops_topic, heartbeat_topic
        self.broadcaster = broadcaster
        self.max_events = max_events
        self.events: OrderedDict[tuple[str, str, int], dict] = OrderedDict()
        self.by_key: dict[tuple[str, str], list[dict]] = {}
        self.batches: deque = deque(maxlen=500)
        self.arrivals: deque = deque(maxlen=100_000)
        self.last_heartbeat_ms: int | None = None
        self.started_at = time.time()
        self.running = False
        self.error: str | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        threading.Thread(target=self._run, name="lakeflow-tracer", daemon=True).start()

    def _consumer(self) -> Consumer:
        consumer = Consumer({"bootstrap.servers": self.bootstrap, "group.id": "lakeflow-api-tracer",
                             "enable.auto.commit": False, "session.timeout.ms": 10000})
        metadata = consumer.list_topics(timeout=10)
        assignment = [
            TopicPartition(topic, partition, OFFSET_END)
            for topic in self.topics
            if topic in metadata.topics
            for partition in metadata.topics[topic].partitions
        ]
        if not assignment:
            consumer.close()
            raise RuntimeError("no LakeFlow topics yet")
        consumer.assign(assignment)
        return consumer

    def _run(self) -> None:
        while True:
            try:
                consumer = self._consumer()
                self.running, self.error = True, None
                while True:
                    message = consumer.poll(1.0)
                    if message is None:
                        continue
                    if message.error():
                        continue
                    self._handle(message)
            except Exception as exc:  # noqa: BLE001 - keep tailing; report state via health
                self.running, self.error = False, f"{type(exc).__name__}: {exc}"
                log.warning("tracer restarting: %s", self.error)
                time.sleep(5)

    def _handle(self, message) -> None:
        topic = message.topic()
        _, ts_ms = message.timestamp()
        if topic == self.ops_topic:
            try:
                record = json.loads(message.value())
            except (TypeError, ValueError):
                return
            with self._lock:
                self.batches.append(record)
            self.broadcaster.publish("batch", record)
            return
        if topic == self.heartbeat_topic:
            self.last_heartbeat_ms = ts_ms
            return
        headers = {k: (v.decode() if isinstance(v, bytes) else v) for k, v in (message.headers() or [])}
        event = summarize_record(topic, message.partition(), message.offset(), ts_ms, message.key(), message.value(), headers)
        with self._lock:
            self.arrivals.append((time.time(), topic))
            self.events[(topic, str(message.partition()), message.offset())] = event
            if event.get("key"):
                self.by_key.setdefault((topic, event["key"]), []).append(event)
            while len(self.events) > self.max_events:
                (old_topic, _, _), old = self.events.popitem(last=False)
                bucket = self.by_key.get((old_topic, old.get("key")))
                if bucket:
                    if old in bucket:
                        bucket.remove(old)
                    if not bucket:
                        self.by_key.pop((old_topic, old.get("key")), None)
        self.broadcaster.publish("cdc_event", event)

    def events_for(self, topic: str, key: str) -> list[dict]:
        with self._lock:
            return list(self.by_key.get((topic, key), []))

    def recent(self, limit: int = 50, topic: str | None = None) -> list[dict]:
        with self._lock:
            items = [e for e in reversed(self.events.values()) if topic is None or e["topic"] == topic]
        return items[:limit]

    def recent_batches(self, limit: int = 50) -> list[dict]:
        with self._lock:
            return list(self.batches)[-limit:]

    def rate(self, window_s: float = 60.0) -> tuple[float, int]:
        cutoff = time.time() - window_s
        with self._lock:
            count = sum(1 for t, topic in self.arrivals if t >= cutoff and topic in self.cdc_topics)
        observed = min(window_s, max(time.time() - self.started_at, 1e-6))
        return count / observed, count


def summarize_record(topic, partition, offset, ts_ms, key, value, headers) -> dict:
    """Pull trace identifiers out of a raw Debezium record without validating it (the pipeline does that)."""
    event = {"topic": topic, "partition": partition, "offset": offset, "kafka_ts_ms": ts_ms, "key": None, "lsn": None,
             "tx_id": None, "op": None, "source_ts_ms": None, "debezium_ts_ms": None, "snapshot": None,
             "injection_id": headers.get("lakeflow-injection-id"), "tombstone": value is None}
    try:
        key_doc = json.loads(key) if key else None
        if isinstance(key_doc, dict) and len(key_doc) == 1:
            event["key"] = str(next(iter(key_doc.values())))
        if value is not None:
            doc = json.loads(value)
            source = doc.get("source") or {}
            event.update(lsn=source.get("lsn"), tx_id=source.get("txId"), op=doc.get("op"), source_ts_ms=source.get("ts_ms"),
                         debezium_ts_ms=doc.get("ts_ms"), snapshot=source.get("snapshot"))
    except (TypeError, ValueError, AttributeError):
        event["malformed"] = True
    return event
