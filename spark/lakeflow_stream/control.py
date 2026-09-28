"""File-based control channel used by the Recovery Lab (no Docker socket needed).

The API writes `<control>/requests/<id>.json`, and the driver polls that directory. `crash_now` exits
immediately. `crash_after_commit` exits after the next batch's Iceberg commits but *before* Spark writes the
checkpoint commit marker, which is the exact window where naive sinks create duplicates. The container
restart policy brings the job back, and the batch is replayed from the checkpoint.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("lakeflow.control")
EXIT_CODE = 137


class ControlChannel:
    def __init__(self, directory: str):
        self.requests = Path(directory) / "requests"
        self.acks = Path(directory) / "acks"
        self.crash_after_commit: dict | None = None
        self._lock = threading.Lock()

    def start(self, interval_s: float = 1.0) -> None:
        self.requests.mkdir(parents=True, exist_ok=True)
        self.acks.mkdir(parents=True, exist_ok=True)
        threading.Thread(target=self._loop, args=(interval_s,), name="lakeflow-control", daemon=True).start()

    def _loop(self, interval_s: float) -> None:
        while True:
            try:
                self.poll_once()
            except Exception:  # noqa: BLE001 - the control plane must never kill the job by accident
                log.exception("control poll failed")
            time.sleep(interval_s)

    def poll_once(self) -> None:
        for path in sorted(self.requests.glob("*.json")):
            try:
                request = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                log.warning("ignoring unreadable control request %s", path.name)
                path.unlink(missing_ok=True)
                continue
            path.unlink(missing_ok=True)
            action = request.get("action")
            if action == "crash_now":
                self.ack(request, {"note": "exiting immediately"})
                log.warning("control: crash_now requested (%s)", request.get("id"))
                os._exit(EXIT_CODE)
            elif action == "crash_after_commit":
                with self._lock:
                    self.crash_after_commit = request
                self.ack(request, {"note": "armed; will exit after the next non-empty batch commits to Iceberg"})
            else:
                self.ack(request, {"error": f"unknown action {action!r}"})

    def maybe_crash_after_commit(self, batch_id: int, applied_events: int) -> None:
        with self._lock:
            request = self.crash_after_commit
            if request is None or applied_events == 0:
                return
            self.crash_after_commit = None
        self.ack(request, {"batch_id": batch_id, "note": "Iceberg committed, checkpoint commit skipped; exiting"})
        log.warning("control: crash_after_commit fired after batch %s", batch_id)
        os._exit(EXIT_CODE)

    def ack(self, request: dict, detail: dict) -> None:
        payload = {**request, **detail, "executed_at": datetime.now(timezone.utc).isoformat()}
        target = self.acks / f"{request.get('id', 'unknown')}.json"
        tmp = target.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, target)
