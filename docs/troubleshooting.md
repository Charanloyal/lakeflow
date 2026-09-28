# Troubleshooting

Start with `make doctor`. It checks Docker, Compose, available memory, `.env`, and the health of every container.
Then `make status` and `make logs SERVICE=<name>`.

| Symptom | Likely cause | Fix |
|---|---|---|
| `bootstrap` says Docker has too little memory | Docker Desktop's default VM size | Raise Docker memory to at least 6 GB for `8gb`, or 11 GB for `16gb` |
| `postgres` unhealthy right after a secrets change | Init scripts only run on an empty volume, so the roles still have the old passwords | `make clean` (deletes volumes), then `make up` |
| `kafka-init` exits non-zero | A topic already exists with a different partition count after `topics.conf` changed | `make clean` for local data, or follow ADR-0001 for a planned repartition |
| `connect-init` never reaches RUNNING | PostgreSQL auth, or the `lakeflow_cdc` publication is missing | `make logs SERVICE=connect`; check the `debezium` role in `.env` |
| `spark` restarts in a loop | Catalog unreachable, or a bad contract file | `make logs SERVICE=spark`; contracts are validated at startup and on hot reload |
| `trino` queries fail with S3 errors | MinIO credentials in `.env` do not match the `minio-data` volume | Restore the original `.env`, or `make clean` |
| UI shows **Dependency unavailable** | The API reports which dependency failed; the UI never shows fake data instead | Fix that service; the page recovers on its next poll |
| Overview metric shows **stale** | The value's timestamp is older than its freshness window | Usually Spark is down or idle; see `runbooks/stream-recovery.md` |
| Recovery Lab buttons are disabled | You are logged in as the viewer role | Log in as the admin user from `.env` |
| Airflow tasks fail with Trino commit conflicts | Maintenance overlapped heavy stream writes | Airflow retries them; see `runbooks/maintenance.md` |
| `make integration-test` times out waiting for silver | The stream is slow on a small machine | Use `PROFILE=8gb`, close other apps, check lag on the Overview page |

## Resetting everything

```bash
make clean        # stops containers and deletes all volumes (data, checkpoints, catalog)
make bootstrap    # keeps .env unless you pass --force (regenerating secrets requires a clean volume set)
make up
```

## Getting diagnostics for an issue report

```bash
make status > status.txt
docker compose logs --no-color --tail 200 > logs.txt
```

Before sharing `logs.txt`, check it for secrets. The logs contain no passwords, but connection strings and
hostnames do appear.
