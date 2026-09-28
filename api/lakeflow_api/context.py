from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from lakeflow_core.contracts import ContractRegistry, contracts_fingerprint, load_registry

from .clients import Clients
from .control_store import ControlStore
from .domain.ratelimit import RateLimiter
from .monitor import Monitor
from .settings import Settings
from .tracer import Broadcaster, Tracer


@dataclass
class AppContext:
    settings: Settings
    clients: Clients
    store: ControlStore
    tracer: Tracer
    monitor: Monitor
    broadcaster: Broadcaster
    mutation_limiter: RateLimiter = field(default_factory=lambda: RateLimiter(60, 60))
    recovery_limiter: RateLimiter = field(default_factory=lambda: RateLimiter(10, 60))
    login_limiter: RateLimiter = field(default_factory=lambda: RateLimiter(10, 60))
    _registry: ContractRegistry | None = None
    _cache: dict = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def registry(self) -> ContractRegistry:
        """Active contracts, reloaded when files change (same hot-reload rule as the Spark job)."""
        fingerprint = contracts_fingerprint(self.settings.contracts_dir)
        if self._registry is None or self._registry.fingerprint != fingerprint:
            self._registry = load_registry(self.settings.contracts_dir)
        return self._registry

    def cached(self, key: str, ttl_s: float, producer):
        now = time.monotonic()
        with self._lock:
            hit = self._cache.get(key)
            if hit and now - hit[0] < ttl_s:
                return hit[1]
        value = producer()
        with self._lock:
            self._cache[key] = (now, value)
        return value


def build_context(settings: Settings) -> AppContext:
    clients = Clients(settings)
    broadcaster = Broadcaster()
    tracer = Tracer(
        settings.kafka_bootstrap,
        settings.cdc_topics + (settings.replay_topic,),
        settings.ops_topic,
        settings.heartbeat_topic,
        broadcaster,
    )
    store = ControlStore(clients)
    monitor = Monitor(settings, clients, tracer, store)
    return AppContext(settings, clients, store, tracer, monitor, broadcaster)
