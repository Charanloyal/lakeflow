# Mid-level readiness review (2026-10-09)

A review of LakeFlow as a mid-level Data Engineer portfolio project: what is verified, what is broken, what is
only claimed, and what was changed in this increment. Historical findings from the
[2026-09-25 audit](../audit/2026-09-25-repository-audit.md) are kept separate from current defects.

## Scope and environment

| | |
|---|---|
| Reviewed commit | `29847ea8ea034b4908cb232cecc6d3e4763331cf` (`main` = `origin/main` = `lakeflow-v2` at review time) |
| Live demo | `https://lakeflow-demo.onrender.com/lakeflow/` proxies the GitHub Pages build. Its `snapshot/meta.json` (fetched 2026-10-09) names commit `29847ea`, captured `2026-09-29T11:43:18Z` by [CI run 36561956920](https://github.com/Charanloyal/lakeflow/actions/runs/36561956920). The Render dashboard shows deploy `30d104a` because Render serves only the nginx proxy, and the later commit changed only the README. |
| Inspected | README, audit, ADR-0001/0003/0005, runbooks, security page, contracts, `libs/lakeflow_core`, `spark/lakeflow_stream`, API services/routes/settings, frontend snapshot mode and recorder, Render nginx config, benchmark harness, all test suites and workflows. |
| Review machine | Windows, Python 3.14 (CI uses 3.10 and 3.12), git. **No Docker, Java, Node/npm, ruff or pytest**; PyPI and npm are unreachable behind an authenticated proxy. VS Code's bundled Node 24 and TypeScript 6.0 were used for the checks listed below. |
| Not inspected | GitHub Actions logs (only the published snapshot was read), the Render service settings, Grafana dashboards in a browser. |

## Baseline (before changes)

| Command | Outcome |
|---|---|
| `python -m unittest discover -s tests/unit` (PYTHONPATH=`libs;api`; same runner as `make test`) | **PASS** 66 tests (Python 3.14) |
| `python -m unittest api.tests.test_domain` | **PASS** 15 tests |
| `python -m pytest -q tests/unit api/tests/test_domain.py` (CI command) | **BLOCKED**: pytest cannot be installed (proxy 407). Same tests ran via `unittest` above. |
| `python -m pytest -q api/tests/test_app.py` | **BLOCKED**: FastAPI/pydantic/psycopg not installable. |
| `ruff check . && ruff format --check .` | **BLOCKED**: ruff not installable. |
| `docker compose --profile tools run --rm spark-tests` | **BLOCKED**: no Docker/Java. |
| `python scripts/lakeflowctl.py --profile 8gb integration-test tests/integration` | **BLOCKED**: no Docker. Needs at least 6 GB of Docker memory (`8gb` profile). |
| `python scripts/lakeflowctl.py --profile 16gb integration-test tests/e2e`, benchmark, Playwright | **BLOCKED**: no Docker; needs about 11 GB (`16gb` profile). |
| `cd frontend && npm ci && npx eslint . && npm run build` | **BLOCKED**: no npm registry access. |

CI selects suites explicitly (`.github/workflows/ci.yml`). Unit and API tests run on every push and PR (3.10 and
3.12). PySpark runs in its runtime image. Integration (`tests/integration`) runs in `data-plane`. E2E, the
benchmark and Playwright run in `full-stack`. Two tests are `xfail` (backfill, see U2). Before this change CI did
not report skip/xfail counts and kept no JUnit or benchmark files as artifacts.

## Confirmed strengths (verified by reading code and running the stdlib suites)

- **Deterministic event identity.** `event_id = sha256(table|pk|lsn|txId|op|row_hash)` excludes Kafka
  coordinates and Debezium processing time (`libs/lakeflow_core/identity.py`, `test_decode.py`).
- **One executable spec, checked against Spark.** `libs/lakeflow_core/semantics.py` defines the sink rules.
  `spark/tests/test_sink.py::test_spark_matches_reference_model` replays random interleavings through both.
- **Idempotent sink.** LSN-guarded silver MERGE with tombstones, bronze anti-join append, DLQ insert-if-absent
  by Kafka coordinates, batch record upsert with `attempts`. Crash-after-commit replay keeps its labels through
  `_prev_source_lsn` (`spark/lakeflow_stream/sink.py`).
- **Honest delivery wording.** At-least-once delivery and an effectively-once outcome are stated in ADR-0003. A
  hygiene test (`tests/unit/test_repo_hygiene.py`) fails when an exactly-once claim is not qualified, and on v1 claims.
- **Loud failure on data loss.** `failOnDataLoss=true` is the default (`spark/lakeflow_stream/config.py`).
- **Bounded, authorized fault injection.** Admin role, CSRF header for cookie sessions, per-user rate limit,
  bounded counts, audit log, `lakeflow-injection-id` header (`api/lakeflow_api/services/recovery.py`, `api/tests/test_app.py`).
- **Recorded demo is read-only by construction.** In snapshot builds every non-GET request throws before any
  network call (`frontend/lib/api.ts`), and `/api/auth/me` is pinned to a `viewer` identity.
- **Benchmark harness measures the real stack** with seeded workloads, per-event samples, pooled
  (event-weighted) percentiles, loss/duplicate/error accounting against the changes it committed, and an
  identical-result check for the before/after `optimize` query (`benchmarks/run.py`, `libs/lakeflow_core/benchmark.py`).

## Confirmed defects (current code)

### D1: DLQ listing exposed direct PII to every viewer (P0, privacy). **Fixed**
- **Evidence.** `api/lakeflow_api/services/quality.py:82` returned `substr(raw_value, 1, 2000)` to any
  authenticated viewer. The Quality page rendered it (`frontend/app/quality/page.tsx`). The raw value is the
  untouched Debezium envelope.
- **Reproduction.** The source accepts any text as e-mail (`platform/postgres/init/01-source-schema.sh:13`),
  while contract v1 requires `format: email` (`contracts/customers/v1.schema.json:22`). A customer with
  e-mail `x at y` becomes a `CONTRACT_VIOLATION` DLQ row whose `raw_value` holds the plaintext `email` and
  `full_name`. `tests/unit/test_redaction.py::DlqPrivacyPathTests` reproduces this through the real decoder.
- **Impact.** It contradicted README/`docs/security.md` ("full names are dropped before they reach the lakehouse"). The CI
  recorder logs in as admin and publishes every GET it sees. The currently published snapshot contains only
  `orders` DLQ rows (checked 2026-10-09), so no PII was published, but nothing prevented it.
- **Fix.** `libs/lakeflow_core/redaction.py` masks contract direct-PII fields and fields unknown to the
  contract. It fails closed: a payload of a PII-bearing or unknown contract that is not a parseable Debezium
  envelope (flat row, schema-wrapped, binary) is withheld. This case came from an independent code review of this
  change. The exact bytes stay in `ops.dlq_events`, so **replay is unchanged**. Orders payloads (no direct PII)
  are still shown byte-exact.
- **Acceptance.** `tests/unit/test_redaction.py` (13 tests, PASS), `api/tests/test_app.py::test_dlq_listing_masks_direct_pii_for_every_role` (CI).

### D2: record trace returned the PostgreSQL row with plaintext PII (P0, privacy). **Fixed**
- **Evidence.** `api/lakeflow_api/services/trace.py:43` ran `SELECT *` on the source table and `:105` returned
  it as `source_state` to viewers. `/api/trace/customers/{id}` therefore exposed `email` and `full_name`.
- **Fix.** `mask_row` applies the same masking before the response. **Acceptance.**
  `api/tests/test_app.py::test_trace_masks_direct_pii_in_the_source_row` (CI). A stubbed service-level run
  passed locally (see Validation).

### D3: recorded pages presented capture-time health as current (P0, presentation). **Fixed**
- **Evidence.** `frontend/components/ui.tsx:24` printed `as of HH:MM:SS` (time of day only), so a ten-day-old
  value read as current. `frontend/app/page.tsx:19,23` said "Live health" and showed `platform healthy` from the
  snapshot. Recorded data never became `stale` because the client stamps it with the load time.
- **Fix.** In snapshot builds the provenance line shows `recorded <date time>`. Overview/Pipeline say the status is
  from capture time. The banner states "Recorded demo, not a live system" and that health/lag/freshness are as captured.
- **Acceptance.** `tsc` (stubbed React types) and syntax checks PASS locally. `next build`, ESLint and Prettier
  run in CI. No browser test of the snapshot build exists yet (see Remaining work).

### D4: absent, malformed or stale provenance was not handled (P1). **Fixed**
- **Evidence.** `frontend/components/AppShell.tsx:171` called `meta.sha.slice(0, 7)` on unvalidated JSON
  (`frontend/lib/api.ts:37`). A `meta.json` without `sha` throws during render. A missing file silently dropped
  the provenance. URLs were rendered as `href` without validation. The capture age was never shown.
- **Fix.** `frontend/lib/provenance.ts` validates sha, timestamp, https links and `mode`, and reports
  `ok | stale | missing | malformed`. The banner always shows the capture age. The stale threshold is an explicit
  **assumption** (30 days, `NEXT_PUBLIC_SNAPSHOT_STALE_AFTER_DAYS`), because no documented policy exists.
- **Acceptance.** `frontend/lib/provenance.test.mjs` (8 tests, PASS on Node 24; CI runs `npm run test:unit`).

### D5: CI evidence was not inspectable (P1, evidence). **Fixed going forward**
- **Evidence.** `ci.yml` uploaded only Playwright output. JUnit XML (`.artifacts/*.xml`) and benchmark raw JSON
  were discarded, and the job summary never showed skips or expected failures. The published benchmark
  (run `a39a6301179a`) can be read only as an API summary inside the recorded demo. Its raw samples are gone.
- **Fix.** Every suite writes a passed/failed/skipped/xfailed table to the job summary
  (`scripts/ci_annotate.py summary`). JUnit and `benchmarks/results/*.json` are uploaded. The recorded snapshot is
  gated by `scripts/check_recorded_demo.py` (provenance, manifest, `.env` secret values, secret patterns,
  non-reserved e-mail domains, unredacted direct-PII fields) on every full-stack run, before anything is published.
- **Acceptance.** `tests/unit/test_ci_annotate.py` (2 tests), `tests/unit/test_recorded_demo_check.py` (9 tests), PASS.
  The gate run against the **currently published** snapshot reported one problem, the missing `mode` field
  (new in this change and written by CI from now on). Secret, PII, e-mail and manifest checks passed.

## Documentation gaps (current)

| # | Gap | Evidence | Resolution |
|---|---|---|---|
| G1 | Freshness was defined as "bronze Iceberg commit − PostgreSQL commit". The sink timestamp is the Spark driver clock read after the silver MERGE commits and **before** the bronze append commits. | `spark/lakeflow_stream/sink.py:495,501,516`; README:87; ADR-0005:19 | Documented as a proxy in README and ADR-0005, with the clock assumption. |
| G2 | "Retention is shortened to 1 h" mixed Iceberg snapshot/orphan retention (1 h) with Kafka CDC retention (7 d). | README:140; `platform/kafka/topics.conf:3` | Reworded with both values. |
| G3 | Recovery limits were not explicit: Spark down longer than Kafka retention, slot invalidation at `max_slot_wal_keep_size=2GB`, source reset (LSN regression), deletes not repaired by snapshots. | `docker-compose.yml:19`; runbook | Added a recovery-limits table and destructive-action warnings to `docs/runbooks/stream-recovery.md`. |
| G4 | No statement that the sink's writes are not one transaction (silver can briefly lead bronze, DLQ and the batch record). | `sink.py` write order | ADR-0003 now lists the commit boundaries and what converges. |
| G5 | The DLQ keeps raw bytes (possibly direct PII) with no retention policy. | `ops.dlq_events`; maintenance only expires snapshots | Documented as a restricted quarantine and an open production gap (not implemented). |
| G6 | README did not say where the Render demo comes from or which commit it shows. | Render dashboard vs `meta.json` | README Demo section explains it. |
| G7 | "What I personally designed" is still a template. | README | Kept, with an explicit *author input needed* note and a structured case-study outline. Nothing filled in on the author's behalf. |

## Unverified claims (not provable in this review)

| # | Claim | Status | How to verify |
|---|---|---|---|
| U1 | README reliability table: "executed in CI against the real stack". | **Partly evidenced.** The recorded demo of `29847ea` was published by run 36561956920. The publish step runs only after the e2e, benchmark and Playwright steps of `full-stack` succeed, so those passed for that commit. The `data-plane` job (`test_03`…`test_09`) is a separate job whose result was not inspected. | Open the run's jobs and the new suite-outcome summary. |
| U2 | Backfill via incremental snapshot. | **Experimental, xfail kept.** Not run here. Whether CI currently reports XFAIL or XPASS is unknown. Hypotheses, none verified: incremental-snapshot events arrive without `source.lsn` and are rejected as `INVALID_ENVELOPE`; or the event lands outside the test's 240 s window. | Read the `integration` row of the new summary. If it is consistently XPASS, remove the marker. Otherwise inspect the DLQ for `op=r` rejects. |
| U3 | Debezium semantics: one LSN per change; snapshot reads carry the snapshot/stream position. | Based on connector behaviour, not observed here. The new semantics tests pin what the sink does in each case, including equal-LSN ties. | An integration test that asserts `source.lsn` and `snapshot` on real `op=r` events. |
| U4 | Benchmark numbers. | One CI run is visible in the published snapshot (`a39a6301179a`, 3 × 300 events, seed 42): 900/900 observed, 0 loss, duplicates and errors, 0 mismatches, p50 7 961 ms / p95 10 711 ms freshness (proxy, see G1), throughput mean 36.5 events/s over 3 iterations (**a smoke floor on a shared runner, not a capacity claim**). Raw samples were not preserved (D5). | Re-run; the raw JSON is now uploaded as an artifact. |
| U5 | Recorded build behaviour in a browser (banner, read-only, routing). | Not executed. The CI Render smoke test asserts `/` → 302 `/lakeflow/` and `/lakeflow/quality` → 301 with a trailing slash; no routing defect was reproduced. | Add a Playwright project against the static snapshot build. |

## Historical audit (2026-09-25, v1): kept separate

The audit's F1–F13 describe the v1 simulation (fabricated throughput, exactly-once claimed without qualification,
hard-coded secrets, simulated site). They are historical. The current code replaces those components, and
`tests/unit/test_repo_hygiene.py` guards the specific v1 claims and secrets. This review did not re-check each F-item
individually. One audit plan item is still open: M5 says benchmark raw JSON from CI runs would be committed
with the run URL, but `benchmarks/results/` holds only `.gitkeep` (see D5).

## Correctness review targets

| Area | Finding | Coverage added |
|---|---|---|
| Identity and ordering | Identity is deterministic, and distinct changes to one key differ by LSN. The per-key order is (LSN, Kafka offset). Offset only breaks equal-LSN ties within one topic partition. | `OrderingTests`: arrival-order independence, equal-LSN tie, connector-restart suffix replay, LSN regression (characterization of an unsupported case). |
| Deletes and snapshots | Tombstones keep their LSN, so no older update or snapshot read resurrects a row. Legitimate recreation (higher LSN) works. Snapshot reads use real LSNs; no artificial "large" order. | `DeleteAndReplayTests`, `SnapshotTests`. |
| Partial commits | Write order: silver MERGE per table → bronze append → DLQ MERGE → batch record → Spark `commits/N`. Each is idempotent; there is no multi-table atomicity. | `PartialCommitTests` (reference model), `spark/tests/test_sink.py::test_crash_between_silver_and_bronze_converges_on_replay` (Spark, CI only). |
| DLQ and privacy | Reason codes, stable `dlq_id`, replay provenance and idempotency are sound. Privacy gaps D1/D2 are fixed for display. Retention (G5) is open. | `test_redaction.py`, API tests, recorded-demo gate. |
| Retention and recovery | See G2/G3. Compaction/expiry is safe relative to the stream: orphan cleanup uses a 1 h floor and Spark batches commit within seconds. Expiry limits time travel only. | Runbook table. |
| Fault injection | Admin-only, CSRF, rate-limited, bounded, audited. `LAKEFLOW_RECOVERY_LAB_ENABLED` defaults to `true` in every environment. | Not changed: a follow-up item (R3). |

## Validation of this change

| Command | Outcome |
|---|---|
| `python -m unittest discover -s tests/unit` | **PASS** 104 tests (66 existing + 38 new: redaction 13, recorded-demo gate 9, CI outcomes 2, semantics 14) |
| `python -m unittest api.tests.test_domain` | **PASS** 15 tests |
| Service smoke run of `quality.rejected` and `trace.trace` with fake clients and a stub for `fastapi.HTTPException` | **PASS** (masked / withheld / none; trace source row masked) |
| `node --experimental-strip-types --test lib/provenance.test.mjs` (VS Code's Node 24) | **PASS** 8 tests |
| TypeScript 6.0 strict check of `lib/provenance.ts` and `lib/api.ts` | **PASS** 0 diagnostics |
| TypeScript strict check of the edited `.tsx` files against minimal React/Next stubs | **PASS** for edited files (one stub-only diagnostic in unchanged `GuidedDemo.tsx`) |
| `python -m py_compile` on every changed Python file; workflow YAML parsed with js-yaml | **PASS** |
| `python scripts/check_recorded_demo.py <published snapshot>` | Runs: only the new `mode` requirement fails, as expected |
| `pytest`, `ruff`, ESLint, Prettier, `next build`, PySpark, integration, e2e, Playwright, benchmark | **BLOCKED** locally (see Baseline). They run in CI; formatting is applied by the Autofix workflow on feature branches. |

## Prioritized plan

**Dependency audit (found after the first push).** The PR's `Security / npm audit` failed on a new upstream
advisory, not on this change: `source-map-js` < 1.2.2 (GHSA-68fv-2mgg-jv7q, high), pulled in through `postcss`.
The same check would also fail on `main`'s next scheduled run. `next` 15.5.26 additionally had two moderate
cache-poisoning advisories (GHSA-4jqv-mc3x-m676, GHSA-mcj8-r9mp-w47p), fixed in 15.5.27. The fix bumps `next`
and `eslint-config-next` to 15.5.27 and pins `source-map-js` 1.2.2 through `overrides`. Running the npm registry's
bulk advisory check (the API `npm audit` calls) on the resulting production tree returned no advisories. The
lockfile is regenerated by the Autofix workflow.

**Done in this increment:** D1–D5, G1–G4, G6, and the regression tests above.

**Remaining (not implemented):**
1. **R1 (P1).** A Playwright project against the static snapshot build. Assert the banner on every page, disabled
   actions, no non-GET requests, and the missing/malformed `meta.json` states.
2. **R2 (P1).** Resolve U2: read the xfail/xpass row. Either remove the marker after consistent passes, or narrow
   the cause and add `strict=True` so an unexpected pass is noticed.
3. **R3 (P2).** Default `LAKEFLOW_RECOVERY_LAB_ENABLED` to `false` unless `LAKEFLOW_ENVIRONMENT=local-demo`.
   This is a public behaviour change, so it is left for the owner.
4. **R4 (P2).** DLQ retention and access: a scheduled purge of resolved rows and Trino access control on
   `ops.dlq_events`. This changes a privacy/replay contract and needs the owner's decision.
5. **R5 (P2).** Benchmark: record the bronze snapshot `committed_at` from Iceberg metadata as the true sink time,
   next to the existing proxy.
6. **R6 (P3).** A soak run (hours) with failure injection, published with the same raw-sample schema, only if
   executed.

## Manual follow-ups (GitHub metadata, not changed by any commit)

- **About:** "CDC lakehouse (PostgreSQL → Debezium → Kafka → Spark → Iceberg → Trino) with idempotent,
  LSN-guarded sinks, failure tests and raw-sample benchmarks. Learning/portfolio project."
- **Website:** `https://lakeflow-demo.onrender.com/lakeflow/` (recorded CI demo).
- **Topics:** `change-data-capture`, `debezium`, `kafka`, `spark-structured-streaming`, `apache-iceberg`, `trino`,
  `data-engineering`, `lakehouse`, `fastapi`, `nextjs`.
