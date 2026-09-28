#!/usr/bin/env python3
"""LakeFlow developer CLI (stdlib only). The Makefile targets are thin wrappers around these commands.

    python scripts/lakeflowctl.py bootstrap [--profile 8gb|16gb]
    python scripts/lakeflowctl.py up | down [--volumes] | status | doctor | logs <service>
    python scripts/lakeflowctl.py demo | test | integration-test | benchmark
    python scripts/lakeflowctl.py migrate | contract promote <name> <version>
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"
ENV_EXAMPLE = ROOT / ".env.example"
CORE_SERVICES = ["postgres", "kafka", "connect", "minio", "iceberg-rest", "spark", "trino", "api", "web"]
DATA_PLANE = ["postgres", "kafka", "connect", "minio", "iceberg-rest", "spark", "trino"]
ONE_SHOT = ["kafka-init", "connect-init"]
HINTS = {
    "postgres": "Check `logs postgres`; an init-script error needs `down --volumes` (init only runs on an empty volume).",
    "kafka": "Kafka needs ~600 MB; on the 8gb profile close other apps or raise Docker Desktop memory.",
    "kafka-init": "Topic creation failed; a partition-count mismatch means topics.conf changed, see ADR-0001.",
    "connect": "Kafka Connect waits for kafka-init; check `logs connect` for plugin or broker errors.",
    "connect-init": "The connector did not reach RUNNING; common causes are PostgreSQL auth or a missing publication.",
    "minio": "MinIO requires MINIO_ROOT_PASSWORD of >= 8 characters (bootstrap generates one).",
    "iceberg-rest": "The REST catalog needs PostgreSQL (iceberg_catalog DB) and MinIO; check both are healthy.",
    "spark": "The stream needs Kafka topics and the REST catalog; `logs spark` shows the Python traceback.",
    "trino": "Trino needs ~1.2 GB; catalog errors usually mean MinIO credentials in .env do not match volumes.",
    "api": "The API needs the control database; `logs api` prints which dependency failed.",
    "web": "The web image serves the static UI and proxies /api to the api service.",
}


def fail(message: str, code: int = 1) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(code)


def run(cmd: list[str], check: bool = True, capture: bool = False, **kw) -> subprocess.CompletedProcess:
    env = kw.pop("env", None) or dict(os.environ)
    if hasattr(os, "getuid"):
        env.setdefault("HOST_UID", str(os.getuid()))
        env.setdefault("HOST_GID", str(os.getgid()))
    return subprocess.run(cmd, cwd=ROOT, check=check, text=True, capture_output=capture, env=env, **kw)


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
    return values


def profile_name(args=None) -> str:
    chosen = getattr(args, "profile", None) or os.environ.get("LAKEFLOW_PROFILE") or read_env(ENV_FILE).get("LAKEFLOW_PROFILE")
    chosen = chosen or "16gb"
    if not (ROOT / "infra" / "profiles" / f"{chosen}.env").exists():
        fail(f"unknown profile {chosen!r}; use 8gb or 16gb")
    return chosen


def compose(profile: str, *extra: str) -> list[str]:
    profile_file = ROOT / "infra" / "profiles" / f"{profile}.env"
    cmd = ["docker", "compose", "--env-file", str(ENV_FILE), "--env-file", str(profile_file)]
    for name in filter(None, read_env(profile_file).get("COMPOSE_PROFILES", "").split(",")):
        cmd += ["--profile", name.strip()]
    return cmd + list(extra)


def generated(name: str) -> str:
    if name == "AIRFLOW_FERNET_KEY":
        return base64.urlsafe_b64encode(os.urandom(32)).decode()
    return secrets.token_urlsafe(24)


# ------------------------------------------------------------------------------------------ commands


def cmd_bootstrap(args) -> None:
    check_prerequisites(strict=not args.skip_docker)
    if ENV_FILE.exists() and not args.force:
        print(".env already exists (keeping it). Use --force to regenerate secrets (requires `down --volumes`).")
    else:
        lines = []
        for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                if value == "__GENERATE__":
                    line = f"{key}={generated(key)}"
                elif key == "LAKEFLOW_PROFILE" and args.profile:
                    line = f"{key}={args.profile}"
            lines.append(line)
        ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
        if os.name != "nt":
            ENV_FILE.chmod(0o600)
        print("wrote .env with freshly generated local secrets")
    if not args.skip_docker:
        profile = profile_name(args)
        print(f"pulling and building images for profile {profile} (first run takes several minutes)...")
        run(compose(profile, "pull", "--ignore-buildable", "--quiet"))
        run(compose(profile, "build"))
    env = read_env(ENV_FILE)
    print("\nbootstrap complete. Next: `make up`, then `make demo`.")
    print(f"UI login (local only): {env.get('LAKEFLOW_ADMIN_USER')} / {env.get('LAKEFLOW_ADMIN_PASSWORD')}")


def check_prerequisites(strict: bool = True) -> None:
    problems = []
    if sys.version_info < (3, 10):
        problems.append("Python >= 3.10 is required")
    if shutil.which("docker") is None:
        problems.append("Docker is not installed (https://docs.docker.com/get-docker/)")
    else:
        version = run(["docker", "compose", "version", "--short"], check=False, capture=True)
        if version.returncode != 0:
            problems.append("Docker Compose v2 plugin is missing")
        else:
            match = re.match(r"v?(\d+)\.(\d+)", version.stdout.strip())
            if match and (int(match.group(1)), int(match.group(2))) < (2, 20):
                problems.append(f"Docker Compose >= 2.20 required, found {version.stdout.strip()}")
        info = run(["docker", "info", "--format", "{{.MemTotal}}"], check=False, capture=True)
        if info.returncode != 0:
            problems.append("Docker daemon is not running")
        elif info.stdout.strip().isdigit():
            gib = int(info.stdout.strip()) / 1024**3
            if gib < 5.5:
                problems.append(f"Docker has {gib:.1f} GiB memory; the 8gb profile needs ~6 GiB available to Docker")
            elif gib < 11 and profile_name() == "16gb":
                print(f"WARNING: Docker has {gib:.1f} GiB; consider `--profile 8gb`")
    if problems and strict:
        fail("prerequisites not met:\n  - " + "\n  - ".join(problems))
    for problem in problems:
        print(f"WARNING: {problem}")


def service_states(profile: str) -> dict[str, dict]:
    out = run(compose(profile, "ps", "--all", "--format", "json"), check=False, capture=True).stdout.strip()
    states: dict[str, dict] = {}
    if not out:
        return states
    items = json.loads(out) if out.startswith("[") else [json.loads(line) for line in out.splitlines() if line.strip()]
    for item in items:
        states[item["Service"]] = item
    return states


def wait_for(profile: str, services: list[str], timeout_s: int) -> None:
    deadline = time.time() + timeout_s
    pending = set(services) | set(ONE_SHOT)
    while pending and time.time() < deadline:
        states = service_states(profile)
        for name in list(pending):
            state = states.get(name, {})
            status, health, exit_code = state.get("State"), state.get("Health"), state.get("ExitCode")
            if name in ONE_SHOT:
                if status == "exited" and exit_code == 0:
                    pending.discard(name)
                elif status == "exited":
                    diagnose(profile, name, f"exited with code {exit_code}")
            elif status == "running" and health in ("healthy", "", None):
                pending.discard(name)
            elif status in ("exited", "dead") or (status == "running" and health == "unhealthy" and time.time() > deadline - 60):
                diagnose(profile, name, f"is {status}/{health}")
        if pending:
            print(f"  waiting for: {', '.join(sorted(pending))}")
            time.sleep(5)
    if pending:
        diagnose(profile, sorted(pending)[0], f"not healthy after {timeout_s}s")


def diagnose(profile: str, service: str, problem: str) -> None:
    print(f"\n{service} {problem}. Last log lines:\n", file=sys.stderr)
    run(compose(profile, "logs", "--no-color", "--tail", "40", service), check=False)
    fail(f"{service} {problem}. Hint: {HINTS.get(service, 'see docs/troubleshooting.md')}")


def cmd_up(args) -> None:
    if not ENV_FILE.exists():
        fail(".env is missing; run `make bootstrap` first")
    profile = profile_name(args)
    services = DATA_PLANE + ONE_SHOT if args.data_plane else []
    run(compose(profile, "up", "-d", "--build", *services))
    wait_for(profile, DATA_PLANE if args.data_plane else CORE_SERVICES, args.timeout)
    env = read_env(ENV_FILE)
    print("\nLakeFlow is up (all ports bound to 127.0.0.1):")
    print(f"  Control plane  http://localhost:{env.get('WEB_PORT', '8080')}   ({env.get('LAKEFLOW_ADMIN_USER')} / see .env)")
    print("  API docs       http://localhost:8000/api/docs")
    print("  Trino UI       http://localhost:8088      Spark UI http://localhost:4040")
    if "observability" in " ".join(compose(profile)):
        print("  Grafana        http://localhost:3000      Prometheus http://localhost:9090")
    if "maintenance" in " ".join(compose(profile)):
        print("  Airflow        http://localhost:8089")


def cmd_down(args) -> None:
    extra = ["-v"] if args.volumes else []
    run(compose(profile_name(args), "down", "--remove-orphans", *extra))


def cmd_status(args) -> None:
    run(compose(profile_name(args), "ps", "--all"))


def cmd_logs(args) -> None:
    run(compose(profile_name(args), "logs", "--tail", str(args.tail), *args.services), check=False)


def cmd_doctor(args) -> None:
    check_prerequisites(strict=False)
    if not ENV_FILE.exists():
        print("WARNING: .env missing (run make bootstrap)")
        return
    profile = profile_name(args)
    states = service_states(profile)
    for name in CORE_SERVICES + ONE_SHOT:
        state = states.get(name, {})
        print(f"  {name:14} {state.get('State', 'absent'):10} {state.get('Health', '') or ''}")


def api_call(method: str, path: str, body: dict | None = None, timeout: float = 30.0) -> dict:
    env = read_env(ENV_FILE)
    base = os.environ.get("LAKEFLOW_API_URL", "http://127.0.0.1:8000")
    token = base64.b64encode(f"{env['LAKEFLOW_ADMIN_USER']}:{env['LAKEFLOW_ADMIN_PASSWORD']}".encode()).decode()
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(base + path, data=data, method=method,
                                     headers={"Authorization": f"Basic {token}", "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - local API URL
        return json.loads(response.read() or b"{}")


def wait_trace(order_id: str, stage: str, lsn_at_least: int | None = None, timeout_s: int = 120) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        trace = api_call("GET", f"/api/trace/orders/{order_id}")
        current = trace.get("final_state") or {}
        done = {s["stage"] for s in trace.get("stages", []) if s["status"] == "done"}
        if stage in done and (lsn_at_least is None or (current.get("_source_lsn") or 0) >= lsn_at_least):
            return trace
        time.sleep(2)
    fail(f"order {order_id} did not reach stage {stage} within {timeout_s}s; open the UI Event Explorer for details")
    return {}


def print_trace(trace: dict) -> None:
    for stage in trace.get("stages", []):
        detail = ", ".join(f"{k}={v}" for k, v in (stage.get("details") or {}).items() if v not in (None, ""))
        print(f"    {stage['stage']:<11} {stage['status']:<8} {stage.get('at') or '':<32} {detail}")
    freshness = trace.get("freshness_ms")
    if freshness is not None:
        print(f"    end-to-end freshness: {freshness / 1000:.1f}s (source commit -> Iceberg snapshot)")


def cmd_demo(args) -> None:
    print("1/5 create an order in PostgreSQL")
    created = api_call("POST", "/api/demo/orders", {"amount": "42.00", "currency": "USD", "status": "PENDING"})
    order_id = created["order_id"]
    print(f"    order_id={order_id} txid={created.get('txid')} commit_lsn={created.get('commit_lsn')}")
    trace = wait_trace(order_id, "trino")
    print_trace(trace)
    lsn = int(trace["final_state"]["_source_lsn"])
    print("2/5 update it (PENDING -> PAID)")
    api_call("PATCH", f"/api/demo/orders/{order_id}", {"status": "PAID"})
    trace = wait_trace(order_id, "trino", lsn_at_least=lsn + 1)
    print_trace(trace)
    lsn = int(trace["final_state"]["_source_lsn"])
    if not args.no_crash:
        print("3/5 arm crash-after-commit on Spark, then update again (the stream dies between Iceberg and checkpoint)")
        api_call("POST", "/api/recovery/actions", {"action": "crash_after_commit"})
        api_call("PATCH", f"/api/demo/orders/{order_id}", {"status": "SHIPPED"})
        print("    waiting for Spark to restart and replay the batch...")
        print_trace(wait_trace(order_id, "trino", lsn_at_least=lsn + 1, timeout_s=240))
    print("4/5 delete it")
    api_call("DELETE", f"/api/demo/orders/{order_id}")
    deadline = time.time() + 180
    while time.time() < deadline:
        trace = api_call("GET", f"/api/trace/orders/{order_id}")
        if (trace.get("final_state") or {}).get("is_deleted"):
            print_trace(trace)
            break
        time.sleep(2)
    print("5/5 verify: exactly one silver row and one bronze row per change event")
    checks = api_call("POST", "/api/quality/run", {"checks": ["orders.primary_key_unique", "bronze.event_id_unique"]}, timeout=120)
    for result in checks.get("results", []):
        print(f"    {result['check_id']:<32} {result['status']:<6} value={result['value']}")
    print("\nOpen http://localhost:8080 and use 'Guided demo' to replay this walkthrough visually.")


def cmd_test(args) -> None:
    env = {**os.environ, "PYTHONPATH": str(ROOT / "libs")}
    print("unit tests (stdlib) ...")
    run([sys.executable, "-m", "unittest", "discover", "-s", "tests/unit"], env=env)
    if not args.unit_only:
        profile = profile_name(args)
        print("API tests (container) ...")
        run(compose(profile, "--profile", "tools", "run", "--rm", "--no-deps", "tools", "pytest", "-q", "api/tests"))
        print("Spark tests (container) ...")
        run(compose(profile, "--profile", "tools", "run", "--rm", "--no-deps", "spark-tests"))


def cmd_integration_test(args) -> None:
    profile = profile_name(args)
    run(compose(profile, "--profile", "tools", "run", "--rm", "tools", "pytest", "-q", *args.paths, *args.extra))


def cmd_benchmark(args) -> None:
    profile = profile_name(args)
    run(compose(profile, "--profile", "tools", "run", "--rm", "tools", "python", "benchmarks/run.py",
                "--profile", args.bench_profile, "--iterations", str(args.iterations), "--events", str(args.events),
                "--seed", str(args.seed)))


def cmd_migrate(args) -> None:
    profile = profile_name(args)
    for path in sorted((ROOT / "platform" / "postgres" / "migrations").glob("*.sql")):
        print(f"applying {path.name}")
        run(compose(profile, "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "lakeflow",
                    "-f", f"/migrations/{path.name}"))


def cmd_contract(args) -> None:
    if args.action != "promote":
        fail("only `contract promote <name> <version>` is supported")
    folder = ROOT / "contracts" / args.name
    proposed = folder / "proposed" / f"v{args.version}.schema.json"
    if not proposed.exists():
        fail(f"{proposed.relative_to(ROOT)} does not exist")
    for path in folder.glob("v*.schema.json"):
        doc = json.loads(path.read_text(encoding="utf-8"))
        if doc["x-lakeflow"]["status"] == "current":
            doc["x-lakeflow"]["status"] = "superseded"
            path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    doc = json.loads(proposed.read_text(encoding="utf-8"))
    doc["x-lakeflow"]["status"] = "current"
    target = folder / f"v{args.version}.schema.json"
    target.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    proposed.unlink()
    sys.path.insert(0, str(ROOT / "libs"))
    from lakeflow_core.contracts import load_registry  # noqa: PLC0415 - validate after the change

    registry = load_registry(ROOT / "contracts")
    print(f"promoted {args.name} v{args.version}; active versions {[c.version for c in registry.versions(args.name)]}")
    print("the Spark job hot-reloads contracts at the start of the next micro-batch")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", choices=["8gb", "16gb"], help="resource profile (default from .env)")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("bootstrap")
    p.add_argument("--force", action="store_true")
    p.add_argument("--skip-docker", action="store_true", help="only write .env (used by CI before image builds)")
    p.set_defaults(func=cmd_bootstrap)
    p = sub.add_parser("up")
    p.add_argument("--timeout", type=int, default=600)
    p.add_argument("--data-plane", action="store_true", help="start only the CDC data plane (no API/UI)")
    p.set_defaults(func=cmd_up)
    p = sub.add_parser("down")
    p.add_argument("--volumes", action="store_true", help="also delete all data volumes")
    p.set_defaults(func=cmd_down)
    sub.add_parser("status").set_defaults(func=cmd_status)
    sub.add_parser("doctor").set_defaults(func=cmd_doctor)
    p = sub.add_parser("logs")
    p.add_argument("services", nargs="*")
    p.add_argument("--tail", type=int, default=100)
    p.set_defaults(func=cmd_logs)
    p = sub.add_parser("demo")
    p.add_argument("--no-crash", action="store_true")
    p.set_defaults(func=cmd_demo)
    p = sub.add_parser("test")
    p.add_argument("--unit-only", action="store_true")
    p.set_defaults(func=cmd_test)
    p = sub.add_parser("integration-test")
    p.add_argument("paths", nargs="*", default=["tests/integration", "tests/e2e"])
    p.set_defaults(func=cmd_integration_test)
    p = sub.add_parser("benchmark")
    p.add_argument("--bench-profile", default="laptop")
    p.add_argument("--iterations", type=int, default=3)
    p.add_argument("--events", type=int, default=20000)
    p.add_argument("--seed", type=int, default=42)
    p.set_defaults(func=cmd_benchmark)
    sub.add_parser("migrate").set_defaults(func=cmd_migrate)
    p = sub.add_parser("contract")
    p.add_argument("action")
    p.add_argument("name")
    p.add_argument("version", type=int)
    p.set_defaults(func=cmd_contract)
    args, extra = parser.parse_known_args()
    if extra and args.command != "integration-test":
        parser.error(f"unrecognized arguments: {' '.join(extra)}")
    args.extra = extra
    try:
        args.func(args)
    except subprocess.CalledProcessError as exc:
        fail(f"command failed ({exc.returncode}): {' '.join(map(str, exc.cmd))}", exc.returncode)
    except urllib.error.URLError as exc:
        fail(f"API not reachable ({exc}); is the stack up? Try `make up` or `python scripts/lakeflowctl.py doctor`")


if __name__ == "__main__":
    main()
