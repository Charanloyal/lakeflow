#!/usr/bin/env python3
"""Turn CI evidence into GitHub annotations (readable via the check-runs API without log access).

python scripts/ci_annotate.py junit <report.xml> [title]
python scripts/ci_annotate.py lines <level> <title> < text
python scripts/ci_annotate.py compose [--profile 8gb]
"""

from __future__ import annotations

import json
import subprocess
import sys
import xml.etree.ElementTree as ET  # noqa: S405 - parsing our own JUnit output
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAX_LEN = 3500


def escape(text: str) -> str:
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def emit(level: str, title: str, message: str) -> None:
    if len(message) > MAX_LEN:
        message = message[:1800] + "\n[...]\n" + message[-(MAX_LEN - 1800) :]
    print(f"::{level} title={escape(title)[:120]}::{escape(message)}")


def junit(path: str, prefix: str = "test") -> None:
    if not Path(path).exists():
        emit("warning", f"{prefix}: no report", f"{path} was not produced")
        return
    root = ET.parse(path).getroot()  # noqa: S314 - trusted local file
    count = 0
    for case in root.iter("testcase"):
        for tag in ("failure", "error"):
            node = case.find(tag)
            if node is not None and count < 10:
                name = f"{case.get('classname', '')}.{case.get('name', '')}"
                text = node.text or ""
                errors = [line for line in text.splitlines() if line.startswith("E ")][:25]
                emit(
                    "error",
                    f"{prefix} {tag}: {name}",
                    f"{node.get('message', '')[:1200]}\n--- E lines ---\n"
                    + "\n".join(errors)
                    + f"\n--- tail ---\n{text[-1200:]}",
                )
                count += 1
    suites = list(root.iter("testsuite")) or [root]
    total = sum(int(s.get("tests", 0)) for s in suites)
    failures = sum(int(s.get("failures", 0)) + int(s.get("errors", 0)) for s in suites)
    emit("notice", f"{prefix} summary", f"{total} tests, {failures} failed/errored")


def lines(level: str, title: str) -> None:
    text = sys.stdin.read().strip()
    if text:
        emit(level, title, text)


def compose(profile: str) -> None:
    profile_file = ROOT / "infra" / "profiles" / f"{profile}.env"
    base = ["docker", "compose", "--env-file", ".env", "--env-file", str(profile_file)]
    for line in profile_file.read_text(encoding="utf-8").splitlines():
        if line.startswith("COMPOSE_PROFILES="):
            for name in filter(None, line.split("=", 1)[1].strip().split(",")):
                base += ["--profile", name.strip()]
    out = subprocess.run(base + ["ps", "--all", "--format", "json"], cwd=ROOT, capture_output=True, text=True).stdout
    items = json.loads(out) if out.strip().startswith("[") else [json.loads(x) for x in out.splitlines() if x.strip()]
    emitted = {"error": 0, "warning": 0}
    summary = []
    for item in items:
        service, state, health, code = item["Service"], item.get("State"), item.get("Health"), item.get("ExitCode")
        summary.append(f"{service}: {state} {health or ''} exit={code}")
        bad = (
            state in ("exited", "dead","restarting") and not (state == "exited" and code == 0) or health == "unhealthy"
        )
        level = "error" if bad else "warning"
        if emitted[level] >= 9:
            continue
        logs = subprocess.run(
            base + ["logs", "--no-color", "--tail", "60", service], cwd=ROOT, capture_output=True, text=True
        ).stdout
        if bad or service in ("spark", "connect", "api", "trino", "iceberg-rest"):
            emit(level, f"{service} logs ({state}/{health})", logs)
            emitted[level] += 1
    emit("notice", "compose state", "\n".join(summary))


if __name__ == "__main__":
    command = sys.argv[1]
    if command == "junit":
        junit(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "test")
    elif command == "lines":
        lines(sys.argv[2], sys.argv[3])
    elif command == "compose":
        compose(sys.argv[3] if len(sys.argv) > 3 and sys.argv[2] == "--profile" else "8gb")
