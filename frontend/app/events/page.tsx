"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { Suspense, useState, type FormEvent } from "react";

import { Timeline } from "@/components/Timeline";
import { Badge, Provenance, StateView } from "@/components/ui";
import { formatTime, shortId } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import type { EventsResponse, Trace } from "@/lib/types";

function Diff({ before, after }: { before: Record<string, unknown> | null; after: Record<string, unknown> | null }) {
  const keys = Array.from(new Set([...Object.keys(before ?? {}), ...Object.keys(after ?? {})])).sort();
  if (!keys.length) return <span className="provenance">no row image</span>;
  return (
    <table>
      <thead>
        <tr>
          <th>Column</th>
          <th>Before</th>
          <th>After</th>
        </tr>
      </thead>
      <tbody>
        {keys.map((key) => {
          const b = before?.[key];
          const a = after?.[key];
          const changed = JSON.stringify(b) !== JSON.stringify(a);
          return (
            <tr key={key} style={changed ? { background: "#58a6ff14" } : undefined}>
              <td className="mono">{key}</td>
              <td className="mono">{b === undefined ? "—" : String(b)}</td>
              <td className="mono">{a === undefined ? "—" : String(a)}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function TraceView({ table, keyValue }: { table: string; keyValue: string }) {
  const trace = useApi<Trace>(`/api/trace/${table}/${encodeURIComponent(keyValue)}`, 3000);
  return (
    <section className="panel" aria-labelledby="trace-title">
      <h2 id="trace-title">
        Trace · {table} <span className="mono">{keyValue}</span>
      </h2>
      <StateView {...trace}>
        {(data) => (
          <>
            <Timeline trace={data} />
            <h3>Change events</h3>
            {data.events.length === 0 ? (
              <p className="provenance">No change events observed for this key yet.</p>
            ) : (
              data.events.map((e) => (
                <details key={`${e.source_lsn}-${e.event_id}`} className="metric" style={{ marginBottom: 8 }} open={data.events.length <= 3}>
                  <summary>
                    <strong>{e.op}</strong> · LSN <span className="mono">{e.lsn}</span> ({e.source_lsn}) · tx {e.tx_id ?? "—"} ·{" "}
                    <span className="mono">
                      {e.kafka_topic}/{e.kafka_partition}/{e.kafka_offset}
                    </span>{" "}
                    · batch {e.batch_id ?? "—"} · snapshot {e.snapshot_id ?? "—"} · <Badge status={e.apply_outcome === "applied" ? "done" : "info"}>{e.apply_outcome ?? "not processed"}</Badge>
                    {e.is_late ? <Badge status="warning">late</Badge> : null}
                    {e.injection_id ? <Badge status="demo">{`injected ${e.injection_id}`}</Badge> : null}
                  </summary>
                  <p className="provenance">
                    event_id <span className="mono">{e.event_id ?? "—"}</span> · source ts {formatTime(e.source_ts)} · committed{" "}
                    {formatTime(e.committed_at)} · contract v{e.contract_version ?? "—"}
                  </p>
                  <Diff before={e.before} after={e.after} />
                </details>
              ))
            )}
            {data.rejected.length ? (
              <>
                <h3>Rejected records for this key</h3>
                <pre>{JSON.stringify(data.rejected, null, 2)}</pre>
              </>
            ) : null}
            <h3>Final state (Trino) vs source (PostgreSQL)</h3>
            <div className="grid two">
              <pre>{JSON.stringify(data.final_state, null, 2)}</pre>
              <pre>{JSON.stringify(data.source_state, null, 2)}</pre>
            </div>
          </>
        )}
      </StateView>
    </section>
  );
}

function Explorer() {
  const router = useRouter();
  const params = useSearchParams();
  const selectedTable = params.get("table");
  const selectedKey = params.get("key");
  const [filters, setFilters] = useState({ table: "orders", op: "", outcome: "", key: "", minutes: "60" });
  const [query, setQuery] = useState(filters);
  const search = new URLSearchParams({ limit: "100", minutes: query.minutes, ...(query.table ? { table: query.table } : {}) });
  if (query.op) search.set("op", query.op);
  if (query.outcome) search.set("outcome", query.outcome);
  if (query.key) search.set("key", query.key.trim());
  const events = useApi<EventsResponse>(`/api/events?${search.toString()}`, 10000);

  const submit = (event: FormEvent) => {
    event.preventDefault();
    setQuery(filters);
  };

  return (
    <>
      <h1>Event explorer</h1>
      <p className="subtitle">Every change event stored in bronze, with its Kafka coordinates, batch and apply outcome. Select one to trace it.</p>
      {selectedTable && selectedKey ? <TraceView table={selectedTable} keyValue={selectedKey} /> : null}
      <form className="panel row" onSubmit={submit} aria-label="Event filters">
        <label>
          Table
          <select value={filters.table} onChange={(e) => setFilters({ ...filters, table: e.target.value })}>
            <option value="orders">orders</option>
            <option value="customers">customers</option>
          </select>
        </label>
        <label>
          Operation
          <select value={filters.op} onChange={(e) => setFilters({ ...filters, op: e.target.value })}>
            <option value="">any</option>
            <option value="c">c (insert)</option>
            <option value="u">u (update)</option>
            <option value="d">d (delete)</option>
            <option value="r">r (snapshot)</option>
          </select>
        </label>
        <label>
          Outcome
          <select value={filters.outcome} onChange={(e) => setFilters({ ...filters, outcome: e.target.value })}>
            <option value="">any</option>
            <option value="applied">applied</option>
            <option value="superseded">superseded</option>
            <option value="stale">stale</option>
          </select>
        </label>
        <label>
          Primary key
          <input value={filters.key} onChange={(e) => setFilters({ ...filters, key: e.target.value })} placeholder="uuid" pattern="[A-Za-z0-9-]*" />
        </label>
        <label>
          Window
          <select value={filters.minutes} onChange={(e) => setFilters({ ...filters, minutes: e.target.value })}>
            <option value="15">15 min</option>
            <option value="60">1 h</option>
            <option value="1440">24 h</option>
            <option value="10080">7 days</option>
          </select>
        </label>
        <button type="submit" className="primary">
          Apply filters
        </button>
      </form>
      <section className="panel">
        <StateView {...events} empty={(d) => d.events.length === 0} emptyText="No events match these filters in the selected window.">
          {(data) => (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Committed</th>
                    <th>Op</th>
                    <th>Key</th>
                    <th>LSN</th>
                    <th>Kafka p/offset</th>
                    <th>Batch</th>
                    <th>Outcome</th>
                    <th>Contract</th>
                    <th>Trace</th>
                  </tr>
                </thead>
                <tbody>
                  {data.events.map((e) => (
                    <tr key={e.event_id}>
                      <td>{formatTime(e.committed_at)}</td>
                      <td>{e.op}</td>
                      <td className="mono">{shortId(e.primary_key, 13)}</td>
                      <td className="mono">{e.source_lsn}</td>
                      <td className="mono">
                        {e.kafka_partition}/{e.kafka_offset}
                      </td>
                      <td className="mono">{e.batch_id}</td>
                      <td>
                        <Badge status={e.apply_outcome === "applied" ? "done" : "info"}>{e.apply_outcome}</Badge>
                        {e.is_late ? <Badge status="warning">late</Badge> : null}
                        {e.injection_id ? <Badge status="demo">injected</Badge> : null}
                      </td>
                      <td>v{e.contract_version}</td>
                      <td>
                        <button
                          onClick={() => router.push(`/events/?table=${e.source_table.split(".")[1]}&key=${encodeURIComponent(e.primary_key)}`)}
                          aria-label={`Trace ${e.primary_key}`}
                        >
                          Trace
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
    </>
  );
}

export default function EventsPage() {
  return (
    <Suspense fallback={<div className="state">Loading…</div>}>
      <Explorer />
    </Suspense>
  );
}
