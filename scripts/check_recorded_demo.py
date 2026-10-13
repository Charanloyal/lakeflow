#!/usr/bin/env python3
"""Gate for the recorded demo: run on the captured snapshot before it is published.

python scripts/check_recorded_demo.py frontend/public/snapshot --env .env

Fails (exit 1) when the snapshot
* lacks valid provenance (meta.json: commit sha, capture time, CI run URL, mode "recorded"),
* references response files that do not exist, or recorded something other than a GET API path,
* contains a secret value from the local .env or a well-known secret pattern,
* contains an e-mail address outside the reserved example domains, or a contract direct-PII field
  (customers.email / full_name, from contracts/) with a value that is not redacted.

Stdlib only. The recorded data is synthetic by construction; this check makes that verifiable.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "libs"))

from lakeflow_core.contracts import load_registry  # noqa: E402

COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")
RUN_URL = re.compile(r"^https://github\.com/[\w.-]+/[\w.-]+/actions/runs/\d+(/attempts/\d+)?$")
RESPONSE_FILE = re.compile(r"^r\d{4,}\.json$")
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+)")
# RFC 2606 / RFC 6761 reserved names: never real mailboxes.
SYNTHETIC_DOMAINS = ("example.com", "example.org", "example.net", "example", "invalid", "test", "localhost")
SECRET_PATTERNS = {
    "private key": re.compile(r"-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "AWS access key": re.compile(r"AKIA[0-9A-Z]{16}"),
    "GitHub token": re.compile(r"gh[pousr]_[A-Za-z0-9]{36}"),
}
SECRET_ENV_NAME = re.compile(r"(PASSWORD|SECRET|TOKEN|_KEY)$")
NOT_RECORDABLE = ("/api/auth/login", "/api/auth/logout", "/api/stream/live")
MIN_SECRET_LEN = 8


def check_meta(snapshot: Path) -> list[str]:
    path = snapshot / "meta.json"
    if not path.is_file():
        return ["meta.json is missing: the recorded demo would have no provenance"]
    try:
        meta = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        return [f"meta.json is not valid JSON: {exc}"]
    if not isinstance(meta, dict):
        return ["meta.json is not a JSON object"]
    problems = []
    if not COMMIT_SHA.match(str(meta.get("sha", ""))):
        problems.append("meta.json sha is not a full git commit id")
    try:
        captured = datetime.fromisoformat(str(meta.get("captured_at", "")).replace("Z", "+00:00"))
        if captured.tzinfo is None:
            problems.append("meta.json captured_at has no timezone")
    except ValueError:
        problems.append("meta.json captured_at is not an ISO 8601 timestamp")
    if not RUN_URL.match(str(meta.get("run_url", ""))):
        problems.append("meta.json run_url is not a GitHub Actions run URL")
    for key in ("repo_url", "codespaces_url"):
        if not str(meta.get(key, "")).startswith("https://"):
            problems.append(f"meta.json {key} is not an https URL")
    if meta.get("mode") != "recorded":
        problems.append('meta.json mode must be "recorded"')
    return problems


def check_manifest(snapshot: Path) -> list[str]:
    path = snapshot / "manifest.json"
    if not path.is_file():
        return ["manifest.json is missing: the recorded demo would show no data"]
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        return [f"manifest.json is not valid JSON: {exc}"]
    if not isinstance(manifest, dict) or not manifest:
        return ["manifest.json must be a non-empty object of API path -> response file"]
    problems = []
    for key, name in manifest.items():
        if not isinstance(key, str) or not key.startswith("/api/"):
            problems.append(f"manifest key {key!r} is not an API path")
        elif key.split("?")[0] in NOT_RECORDABLE:
            problems.append(f"manifest records {key!r}, which must never be part of a recording")
        if not isinstance(name, str) or not RESPONSE_FILE.match(name):
            problems.append(f"manifest entry {key!r} points to an unexpected file name {name!r}")
        elif not (snapshot / name).is_file():
            problems.append(f"manifest entry {key!r} points to a missing file {name}")
    return problems


def secret_values(env_file: Path | None) -> dict[str, str]:
    """Secret values from a dotenv file (names ending in PASSWORD/SECRET/TOKEN/_KEY). Values are never printed."""
    if env_file is None or not env_file.is_file():
        return {}
    values = {}
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        name, value = line.split("=", 1)
        value = value.strip().strip('"').strip("'")
        if SECRET_ENV_NAME.search(name.strip()) and len(value) >= MIN_SECRET_LEN:
            values[name.strip()] = value
    return values


def _walk(value, pii_fields: frozenset[str], where: str, problems: list[str]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in pii_fields and isinstance(item, str) and item and not item.startswith("[redacted"):
                problems.append(f"{where}: unredacted direct-PII field {key!r}")
            _walk(item, pii_fields, where, problems)
    elif isinstance(value, list):
        for item in value:
            _walk(item, pii_fields, where, problems)
    elif isinstance(value, str) and value[:1] in "{[":
        try:
            nested = json.loads(value)
        except ValueError:
            return
        _walk(nested, pii_fields, where, problems)


def check_content(snapshot: Path, secrets: dict[str, str], pii_fields: frozenset[str]) -> list[str]:
    problems = []
    for path in sorted(snapshot.glob("*.json")):
        text = path.read_text(encoding="utf-8", errors="replace")
        for name, value in secrets.items():
            if value in text:
                problems.append(f"{path.name}: contains the value of {name} from the env file")
        for label, pattern in SECRET_PATTERNS.items():
            if pattern.search(text):
                problems.append(f"{path.name}: contains a {label}")
        for match in EMAIL.finditer(text):
            domain = match.group(1).lower()
            if not any(domain == d or domain.endswith("." + d) for d in SYNTHETIC_DOMAINS):
                problems.append(f"{path.name}: e-mail address in non-reserved domain {domain!r}")
        try:
            doc = json.loads(text)
        except ValueError:
            problems.append(f"{path.name}: not valid JSON")
            continue
        _walk(doc, pii_fields, path.name, problems)
    return problems


def direct_pii_fields(contracts_dir: Path) -> frozenset[str]:
    registry = load_registry(contracts_dir, include_proposed=True)
    fields = set()
    for contract in registry.names:
        fields.update(name for name, spec in registry.all_fields(contract).items() if spec.pii == "direct")
    return frozenset(fields)


def check(snapshot: Path, env_file: Path | None, contracts_dir: Path) -> list[str]:
    if not snapshot.is_dir():
        return [f"{snapshot} is not a directory"]
    return (
        check_meta(snapshot)
        + check_manifest(snapshot)
        + check_content(snapshot, secret_values(env_file), direct_pii_fields(contracts_dir))
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("--env", type=Path, default=None, help="dotenv file whose secret values must not leak")
    parser.add_argument("--contracts", type=Path, default=ROOT / "contracts")
    args = parser.parse_args(argv)
    problems = check(args.snapshot, args.env, args.contracts)
    files = len(list(args.snapshot.glob("*.json"))) if args.snapshot.is_dir() else 0
    print(json.dumps({"snapshot": str(args.snapshot), "files": files, "problems": problems}, indent=1))
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
