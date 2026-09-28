"use client";

import { useState } from "react";

import { useShell } from "@/components/AppShell";
import { LineChart } from "@/components/LineChart";
import { Badge, Provenance, StateView } from "@/components/ui";
import { api, ApiError } from "@/lib/api";
import { formatDateTime, formatNumber, shortId } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import type { DlqResponse, FreshnessResponse, QualityRun, QualitySummary, SchemaResponse } from "@/lib/types";

export default function QualityPage() {
  const { identity } = useShell();
  const isAdmin = identity?.role === "admin";
  const summary = useApi<QualitySummary>("/api/quality/summary", 15000);
  const dlq = useApi<DlqResponse>("/api/quality/rejected?limit=50", 10000);
  const schema = useApi<SchemaResponse>("/api/quality/schema", 30000);
  const freshness = useApi<FreshnessResponse>("/api/quality/freshness?minutes=60", 15000);
  const [running, setRunning] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  async function runChecks() {
    setRunning(true);
    setMessage(null);
    try {
      const run = await api.post<QualityRun>("/api/quality/run", {});
      const failed = run.results.filter((r) => r.status !== "pass");
      setMessage(`${run.results.length} checks ran; ${failed.length} not passing${run.persisted ? "" : " (results not persisted)"}.`);
      summary.reload();
    } catch (error) {
      setMessage(error instanceof ApiError ? error.message : String(error));
    } finally {
      setRunning(false);
    }
  }

  async function replay(ids: string[]) {
    try {
      const result = await api.post<{ replayed: string[]; missing: string[] }>("/api/quality/dlq/replay", { dlq_ids: ids });
      setMessage(`replayed ${result.replayed.length} record(s) to lakeflow.replay.cdc; missing ${result.missing.length}.`);
      dlq.reload();
    } catch (error) {
      setMessage(error instanceof ApiError ? error.message : String(error));
    }
  }

  return (
    <>
      <h1>Data quality</h1>
      <p className="subtitle">Checks are generated from the versioned contracts and run through Trino (also scheduled by Airflow).</p>
      {message ? (
        <div className="callout" role="status">
          {message}
        </div>
      ) : null}

      <section className="panel" aria-labelledby="checks-title">
        <div className="row" style={{ justifyContent: "space-between" }}>
          <h2 id="checks-title">Contract and reconciliation checks</h2>
          <button className="primary" onClick={runChecks} disabled={!isAdmin || running}>
            {running ? "Running…" : "Run checks now"}
          </button>
        </div>
        <StateView {...summary} empty={(d) => d.results.length === 0} emptyText="No check results yet. Run the checks (admin) or wait for the Airflow validation DAG.">
          {(data) => (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Check</th>
                    <th>Category</th>
                    <th>Status</th>
                    <th>Value</th>
                    <th>Threshold</th>
                    <th>Last run</th>
                  </tr>
                </thead>
                <tbody>
                  {data.results.map((r) => (
                    <tr key={r.check_id}>
                      <td>
                        <div className="mono">{r.check_id}</div>
                        <div className="provenance">{r.description}</div>
                      </td>
                      <td>{r.category}</td>
                      <td>
                        <Badge status={r.status} />
                        {r.error ? <div className="provenance">{r.error}</div> : null}
                      </td>
                      <td>
                        {formatNumber(r.value)} {r.unit}
                      </td>
                      <td>{r.threshold}</td>
                      <td className="provenance">{formatDateTime(r.run_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <Provenance asOf={data.as_of} source={data.source} />
            </div>
          )}
        </StateView>
      </section>

      <section className="panel" aria-labelledby="freshness-title">
        <h2 id="freshness-title">Freshness history (per micro-batch)</h2>
        <StateView {...freshness}>
          {(data) => (
            <>
              <LineChart
                title="Freshness p50 and p95 per batch versus SLA"
                unit="s"
                threshold={{ value: data.sla_p95_seconds, label: `SLA p95 ${data.sla_p95_seconds}s` }}
                series={[
                  {
                    label: "p95",
                    color: "#58a6ff",
                    points: data.points.map((p) => ({ x: new Date(p.committed_at).getTime(), y: p.freshness_p95_ms === null ? null : p.freshness_p95_ms / 1000 })),
                  },
                  {
                    label: "p50",
                    color: "#3fb950",
                    points: data.points.map((p) => ({ x: new Date(p.committed_at).getTime(), y: p.freshness_p50_ms === null ? null : p.freshness_p50_ms / 1000 })),
                  },
                ]}
              />
              <Provenance asOf={data.as_of} source={data.source} windowLabel="60m" />
            </>
          )}
        </StateView>
      </section>

      <section className="panel" aria-labelledby="dlq-title">
        <h2 id="dlq-title">Rejected records (DLQ)</h2>
        <StateView {...dlq} empty={(d) => d.records.length === 0} emptyText="The DLQ is empty. Use the Recovery Lab to inject malformed records.">
          {(data) => (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>First seen</th>
                    <th>Reason</th>
                    <th>Violations</th>
                    <th>Kafka</th>
                    <th>Status</th>
                    <th>Payload</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {data.records.map((r) => (
                    <tr key={r.dlq_id}>
                      <td>{formatDateTime(r.first_seen_at)}</td>
                      <td>
                        <Badge status="error">{r.error_code}</Badge>
                        <div className="provenance">{r.error_detail}</div>
                        {r.injection_id ? <Badge status="demo">{`injected ${shortId(r.injection_id, 18)}`}</Badge> : null}
                      </td>
                      <td className="mono">{(r.violations ?? []).join(", ") || "—"}</td>
                      <td className="mono">
                        {r.kafka_topic}/{r.kafka_partition}/{r.kafka_offset}
                      </td>
                      <td>
                        <Badge status={r.status === "open" ? "warning" : "done"}>{r.status}</Badge>
                        {r.replay_attempts ? <div className="provenance">{r.replay_attempts} replay(s)</div> : null}
                      </td>
                      <td>
                        <details>
                          <summary>raw</summary>
                          <pre>{r.raw_value}</pre>
                        </details>
                      </td>
                      <td>
                        <button disabled={!isAdmin || r.status !== "open"} onClick={() => replay([r.dlq_id])}>
                          Replay
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <Provenance asOf={data.as_of} source={data.source} />
            </div>
          )}
        </StateView>
      </section>

      <section className="panel" aria-labelledby="schema-title">
        <h2 id="schema-title">Contracts and schema evolution</h2>
        <StateView {...schema}>
          {(data) => (
            <div className="grid two">
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Contract</th>
                      <th>Version</th>
                      <th>Status</th>
                      <th>Owner</th>
                      <th>PII fields</th>
                      <th>Compatibility</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.contracts.map((c) => (
                      <tr key={`${c.contract}-${c.version}`}>
                        <td>{c.contract}</td>
                        <td>v{c.version}</td>
                        <td>
                          <Badge status={c.status === "current" ? "done" : c.status === "proposed" ? "pending" : "info"}>{c.status}</Badge>
                        </td>
                        <td className="provenance">{c.owner.team}</td>
                        <td className="mono">
                          {c.fields
                            .filter((f) => f.pii === "direct")
                            .map((f) => `${f.name} → ${f.pii_handling}`)
                            .join(", ") || "none"}
                        </td>
                        <td>
                          {c.compatibility_problems.length ? (
                            <Badge status="fail">{c.compatibility_problems.join("; ")}</Badge>
                          ) : (
                            <Badge status="pass">{c.compatibility}</Badge>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div>
                <h3>Observed versions (bronze)</h3>
                {data.observed_versions.length ? (
                  <ul>
                    {data.observed_versions.map((v) => (
                      <li key={`${v.contract_name}-${v.contract_version}`}>
                        {v.contract_name} v{v.contract_version}: {formatNumber(v.events)} events (last {formatDateTime(v.last_seen)})
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="provenance">No events in bronze yet.</p>
                )}
                <h3>Schema drift (fields no contract declares)</h3>
                {data.drift.length ? (
                  <ul>
                    {data.drift.map((d) => (
                      <li key={`${d.contract_name}-${d.field}`}>
                        <Badge status="warning">{d.field}</Badge> in {d.contract_name}: {d.events} events
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="provenance">No drift observed.</p>
                )}
                <Provenance asOf={data.as_of} source={data.source} />
              </div>
            </div>
          )}
        </StateView>
      </section>
    </>
  );
}
