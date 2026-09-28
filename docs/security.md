# Security model

LakeFlow is a local, single-node demo. This page states what is protected, how, and what is out of scope.

## Controls in place

| Area | Control |
|---|---|
| Secrets | `make bootstrap` generates every password and key into `.env` (mode 0600, git-ignored). `.env.example` holds only placeholders and clearly local UI passwords. `tests/unit/test_repo_hygiene.py` fails CI on committed secrets. |
| Network | Every published port binds to `127.0.0.1`. Service-to-service traffic stays on the compose network. |
| Database roles | Least privilege: `debezium` has REPLICATION plus SELECT and writes only the heartbeat/signal rows. `trino_reader` is read-only on `shop`. The app, catalog, control and Airflow databases each have their own role. |
| API authentication | HTTP Basic for API clients, never cached by browsers (no `WWW-Authenticate` header). The UI uses an HMAC-signed HttpOnly `SameSite=Strict` session cookie. Cookie-authenticated writes require the `X-LakeFlow-CSRF` header. Login is rate-limited. |
| Authorization | `viewer` can read; `admin` is required for demo mutations, DLQ replay, quality runs and fault injection. |
| Fault injection | Bounded counts per action, and every action is audited in the control database. Injected records carry a `lakeflow-injection-id` header, so they never masquerade as real data. |
| Input handling | Every query uses parameters. Identifiers come only from validated contracts. Backfill filters are structured (UUID keys, timestamps) and never raw SQL. |
| PII | Contracts declare PII handling: `email` is HMAC-SHA256-pseudonymized with a per-install key, and `full_name` is dropped before the lakehouse. |
| Web | nginx sends a strict CSP, `X-Content-Type-Options`, `Referrer-Policy` and `frame-ancestors 'none'`. The UI is a static export with no server-side rendering of user input. |
| Supply chain | Every container image and Python/npm dependency is pinned. Spark jars are verified by SHA-1. |

## Out of scope (by design, for a laptop demo)

- TLS between services and at the edge, SSO/OIDC, secret managers and key rotation.
- Multi-tenant isolation, row/column-level security in Trino, and encryption at rest for MinIO.
- Hardening of third-party admin UIs (Trino, Spark, Airflow, Grafana). They bind to localhost only.

Report a vulnerability by opening a private security advisory on the GitHub repository.
