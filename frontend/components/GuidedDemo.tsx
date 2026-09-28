"use client";

import { useEffect, useState } from "react";

import { api, ApiError } from "@/lib/api";
import type { MutationResult, QualityRun, Trace } from "@/lib/types";

import { useShell } from "./AppShell";
import { Timeline } from "./Timeline";
import { Badge } from "./ui";

type StepState = "idle" | "running" | "done" | "error";
const STEPS = [
  { id: "create", title: "Insert an order in PostgreSQL", detail: "INSERT INTO shop.orders — follow it to Trino." },
  { id: "update", title: "Update it (PENDING → PAID)", detail: "A second change event with a higher LSN." },
  {
    id: "crash",
    title: "Crash Spark after the Iceberg commit",
    detail: "Arms crash_after_commit, then updates to SHIPPED. Spark dies before writing commits/N and replays the batch.",
  },
  { id: "delete", title: "Delete it", detail: "Soft-delete tombstone keeps the LSN so older events can never resurrect it." },
  { id: "verify", title: "Verify uniqueness and reconciliation", detail: "Runs contract checks live through Trino." },
] as const;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitForTrace(orderId: string, predicate: (trace: Trace) => boolean, timeoutMs: number): Promise<Trace> {
  const deadline = Date.now() + timeoutMs;
  let last: Trace | null = null;
  while (Date.now() < deadline) {
    try {
      last = await api.get<Trace>(`/api/trace/orders/${orderId}`);
      if (predicate(last)) return last;
    } catch (error) {
      if (error instanceof ApiError && error.status === 401) throw error;
    }
    await sleep(1500);
  }
  throw new Error(`timed out after ${Math.round(timeoutMs / 1000)} s${last ? "" : " (no trace yet)"}`);
}

const trinoDone = (trace: Trace) => trace.stages.some((s) => s.stage === "trino" && s.status === "done");
const lsnOf = (trace: Trace | null) => Number(trace?.final_state?._source_lsn ?? 0);

export function GuidedDemo() {
  const { identity } = useShell();
  const [states, setStates] = useState<StepState[]>(STEPS.map(() => "idle"));
  const [orderId, setOrderId] = useState<string | null>(null);
  const [trace, setTrace] = useState<Trace | null>(null);
  const [log, setLog] = useState<string[]>([]);
  const [withCrash, setWithCrash] = useState(true);
  const [busy, setBusy] = useState(false);
  const [checks, setChecks] = useState<QualityRun | null>(null);

  useEffect(() => {
    if (!orderId) return;
    let stopped = false;
    const tick = async () => {
      try {
        const next = await api.get<Trace>(`/api/trace/orders/${orderId}`);
        if (!stopped) setTrace(next);
      } catch {
        // the step runner reports errors; background refresh is best effort
      }
    };
    void tick();
    const timer = setInterval(tick, 1500);
    return () => {
      stopped = true;
      clearInterval(timer);
    };
  }, [orderId]);

  const note = (line: string) => setLog((previous) => [`${new Date().toLocaleTimeString()}  ${line}`, ...previous]);
  const mark = (index: number, state: StepState) =>
    setStates((previous) => previous.map((value, i) => (i === index ? state : value)));

  async function runAll() {
    setBusy(true);
    setChecks(null);
    setStates(STEPS.map(() => "idle"));
    let id = "";
    let current = 0;
    try {
      mark(0, "running");
      const created = await api.post<MutationResult>("/api/demo/orders", { amount: "42.00", currency: "USD", status: "PENDING" });
      id = created.order_id;
      setOrderId(id);
      note(`inserted order ${id} (txid ${created.txid}, WAL ${created.commit_lsn})`);
      let latest = await waitForTrace(id, trinoDone, 180_000);
      note(`visible in Trino; freshness ${((latest.freshness_ms ?? 0) / 1000).toFixed(1)} s`);
      mark(0, "done");

      current = 1;
      mark(1, "running");
      let lsn = lsnOf(latest);
      await api.patch<MutationResult>(`/api/demo/orders/${id}`, { status: "PAID" });
      latest = await waitForTrace(id, (t) => lsnOf(t) > lsn, 180_000);
      note(`update applied at LSN ${latest.final_state?._source_lsn}`);
      mark(1, "done");

      current = 2;
      if (withCrash) {
        mark(2, "running");
        lsn = lsnOf(latest);
        await api.post("/api/recovery/actions", { action: "crash_after_commit" });
        note("crash_after_commit armed; updating to SHIPPED");
        await api.patch<MutationResult>(`/api/demo/orders/${id}`, { status: "SHIPPED" });
        latest = await waitForTrace(id, (t) => lsnOf(t) > lsn && trinoDone(t), 300_000);
        const spark = latest.stages.find((s) => s.stage === "spark");
        note(`recovered: batch ${String(spark?.details.batch_id)} ran ${String(spark?.details.attempts ?? "?")} times, one silver row`);
        mark(2, "done");
      }

      current = 3;
      mark(3, "running");
      await api.del<MutationResult>(`/api/demo/orders/${id}`);
      latest = await waitForTrace(id, (t) => Boolean(t.final_state?.is_deleted), 180_000);
      note("tombstone visible in Trino (is_deleted = true)");
      mark(3, "done");

      current = 4;
      mark(4, "running");
      const run = await api.post<QualityRun>("/api/quality/run", {
        checks: ["orders.primary_key_unique", "bronze.event_id_unique", "orders.source_reconciliation"],
      });
      setChecks(run);
      mark(4, run.results.every((r) => r.status === "pass") ? "done" : "error");
      note(`checks: ${run.results.map((r) => `${r.check_id}=${r.status}`).join(", ")}`);
    } catch (error) {
      mark(current, "error");
      note(`step failed: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      setBusy(false);
    }
  }

  const isAdmin = identity?.role === "admin";
  return (
    <section className="panel" aria-labelledby="guided-demo">
      <div className="row" style={{ justifyContent: "space-between" }}>
        <h2 id="guided-demo">Guided demo</h2>
        <div className="row">
          <label className="row" style={{ display: "flex" }}>
            <input type="checkbox" checked={withCrash} onChange={(e) => setWithCrash(e.target.checked)} disabled={busy} />
            include Spark crash/recovery
          </label>
          <button className="primary" onClick={runAll} disabled={busy || !isAdmin} data-testid="guided-demo-start">
            {busy ? "Running…" : "Start guided demo"}
          </button>
        </div>
      </div>
      {!isAdmin ? <p className="provenance">Sign in as the admin user to run mutations.</p> : null}
      <ol className="grid" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", padding: 0 }}>
        {STEPS.map((step, index) => (
          <li key={step.id} className="metric" style={{ listStyle: "none" }} data-testid={`demo-step-${step.id}`}>
            <div className="row" style={{ justifyContent: "space-between" }}>
              <strong>
                {index + 1}. {step.title}
              </strong>
              <Badge status={states[index] === "running" ? "pending" : states[index] === "idle" ? "unknown" : states[index]}>
                {step.id === "crash" && !withCrash ? "skipped" : states[index]}
              </Badge>
            </div>
            <div className="provenance">{step.detail}</div>
          </li>
        ))}
      </ol>
      {trace ? <Timeline trace={trace} /> : null}
      {checks ? (
        <ul>
          {checks.results.map((r) => (
            <li key={r.check_id}>
              <Badge status={r.status} /> {r.check_id}: {r.value ?? "—"} (threshold {r.threshold})
            </li>
          ))}
        </ul>
      ) : null}
      {log.length ? <pre aria-live="polite" data-testid="demo-log">{log.join("\n")}</pre> : null}
    </section>
  );
}
