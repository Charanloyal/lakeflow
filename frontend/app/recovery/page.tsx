"use client";

import { useState } from "react";

import { useShell } from "@/components/AppShell";
import { Badge, StateView } from "@/components/ui";
import { api, ApiError } from "@/lib/api";
import { formatDateTime } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import type { RecoveryAction, RecoveryList } from "@/lib/types";

const ACTIONS = [
  { action: "inject_duplicates", title: "Redeliver events", expect: "Real records re-produced byte-for-byte: counted as duplicates, bronze and silver unchanged.", count: true },
  { action: "inject_malformed", title: "Inject malformed records", expect: "Routed to the DLQ with a reason code; the stream keeps running.", count: true, kind: true },
  { action: "inject_late", title: "Inject late, out-of-order updates", expect: "Older LSN and 2 h old timestamp: stored as stale + late, silver unchanged.", count: true },
  { action: "crash_after_commit", title: "Crash Spark after the next Iceberg commit", expect: "Spark exits before the checkpoint commit; the replayed batch must not duplicate rows (attempts = 2).", destructive: true },
  { action: "crash_now", title: "Kill Spark now", expect: "Hard stop; Docker restarts the job and it resumes from the checkpoint.", destructive: true },
  { action: "restart_connector", title: "Restart the Debezium connector", expect: "Resumes from the last flushed LSN; any re-emitted events are absorbed as duplicates.", destructive: true },
  { action: "apply_schema_migration", title: "Apply schema migration (contract v2)", expect: "Adds shop.orders.channel; new events are stamped contract v2.", destructive: true },
] as const;

export default function RecoveryPage() {
  const { identity } = useShell();
  const isAdmin = identity?.role === "admin";
  const history = useApi<RecoveryList>("/api/recovery/actions", 5000);
  const [count, setCount] = useState(3);
  const [kind, setKind] = useState("contract_violation");
  const [result, setResult] = useState<string | null>(null);
  const [pending, setPending] = useState<string | null>(null);

  async function trigger(action: string, destructive: boolean) {
    if (destructive && !window.confirm(`Run ${action}? This affects the running local stack.`)) return;
    setPending(action);
    setResult(null);
    try {
      const response = await api.post<RecoveryAction>("/api/recovery/actions", { action, count, kind });
      setResult(`${response.action} ${response.status}: ${JSON.stringify(response.detail)}`);
      history.reload();
    } catch (error) {
      setResult(error instanceof ApiError ? `${error.status}: ${error.message}` : String(error));
    } finally {
      setPending(null);
    }
  }

  return (
    <>
      <h1>Recovery lab</h1>
      <p className="subtitle">
        Bounded, audited fault injection (admin only, rate-limited). Injected records carry a <code>lakeflow-injection-id</code>{" "}
        header and are labelled everywhere they appear.
      </p>
      <div className="panel row">
        <label>
          Count (1–20)
          <input type="number" min={1} max={20} value={count} onChange={(e) => setCount(Math.max(1, Math.min(20, Number(e.target.value))))} />
        </label>
        <label>
          Malformed kind
          <select value={kind} onChange={(e) => setKind(e.target.value)}>
            <option value="contract_violation">contract violation</option>
            <option value="invalid_json">invalid JSON</option>
            <option value="unsupported_op">unsupported op (truncate)</option>
            <option value="key_mismatch">key mismatch</option>
          </select>
        </label>
        {!isAdmin ? <span className="provenance">Read-only: sign in as admin to run actions.</span> : null}
        {history.data && !history.data.enabled ? <Badge status="down">disabled by LAKEFLOW_RECOVERY_LAB_ENABLED</Badge> : null}
      </div>
      {result ? (
        <div className="callout" role="status" data-testid="recovery-result">
          {result}
        </div>
      ) : null}
      <section className="grid metrics" aria-label="Actions">
        {ACTIONS.map((item) => (
          <div key={item.action} className="metric">
            <strong>{item.title}</strong>
            <p className="provenance">Expected: {item.expect}</p>
            <button
              className={"destructive" in item ? "danger" : "primary"}
              disabled={!isAdmin || pending !== null || history.data?.enabled === false}
              onClick={() => trigger(item.action, "destructive" in item)}
              data-testid={`action-${item.action}`}
            >
              {pending === item.action ? "Running…" : "Run"}
            </button>
          </div>
        ))}
      </section>
      <section className="panel" style={{ marginTop: 16 }}>
        <h2>Action log</h2>
        <StateView {...history} empty={(d) => d.actions.length === 0 && d.acks.length === 0} emptyText="No actions yet.">
          {(data) => (
            <div className="grid two">
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>When</th>
                      <th>Action</th>
                      <th>By</th>
                      <th>Status</th>
                      <th>Detail</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.actions.map((a) => (
                      <tr key={a.id}>
                        <td>{formatDateTime(a.requested_at)}</td>
                        <td className="mono">{a.action}</td>
                        <td>{a.actor}</td>
                        <td>
                          <Badge status={a.status === "done" ? "done" : "pending"}>{a.status}</Badge>
                        </td>
                        <td className="mono">{JSON.stringify(a.detail)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div>
                <h3>Acknowledged by the Spark driver</h3>
                <pre>{JSON.stringify(data.acks, null, 2)}</pre>
              </div>
            </div>
          )}
        </StateView>
      </section>
    </>
  );
}
