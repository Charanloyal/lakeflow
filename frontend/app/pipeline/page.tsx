"use client";

import { Fragment, useEffect, useState } from "react";

import { useShell } from "@/components/AppShell";
import { Badge, Provenance, StateView } from "@/components/ui";
import { formatBytes, formatNumber, formatTime } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import type { BatchesResponse, Topology } from "@/lib/types";

export default function PipelinePage() {
  const shell = useShell();
  const polled = useApi<Topology>("/api/topology", 10000);
  const topology = shell.topology ?? polled.data;
  const batches = useApi<BatchesResponse>("/api/metrics/batches?limit=25", 10000);
  const [pulse, setPulse] = useState<string | null>(null);

  useEffect(() => {
    if (shell.events[0]) {
      setPulse("kafka");
      const timer = setTimeout(() => setPulse(null), 1200);
      return () => clearTimeout(timer);
    }
  }, [shell.events]);
  useEffect(() => {
    if (shell.batches[0]) {
      setPulse("iceberg");
      const timer = setTimeout(() => setPulse(null), 1200);
      return () => clearTimeout(timer);
    }
  }, [shell.batches]);

  return (
    <>
      <h1>Live pipeline</h1>
      <p className="subtitle">
        PostgreSQL WAL → Debezium → Kafka → Spark → Iceberg → Trino. Status comes from the API health monitor (10 s); the
        highlight follows live events from the stream.
      </p>
      <StateView loading={polled.loading && !topology} error={polled.error} data={topology ?? undefined} stale={polled.stale}>
        {(data) => (
          <section className="panel" aria-label="Topology">
            <div className="pipeline">
              {data.nodes.map((node, index) => {
                const edge = data.edges[index];
                return (
                  <Fragment key={node.id}>
                    <div className={`stage-node${pulse === node.id ? " active" : ""}`} data-testid={`node-${node.id}`}>
                      <h3>{node.label}</h3>
                      <Badge status={node.status} />
                      <p className="provenance">{node.detail}</p>
                      {node.metrics.map((metric) => (
                        <div key={metric.name} style={{ marginTop: 6 }}>
                          <div className="provenance">{metric.label}</div>
                          <div className="mono">{metric.value === null ? "—" : String(metric.value)}</div>
                          <Provenance asOf={metric.as_of} source={metric.source} />
                        </div>
                      ))}
                    </div>
                    {edge && index < data.nodes.length - 1 ? (
                      <div className="connector" aria-label={`${edge.label}`}>
                        <span className="arrow" aria-hidden="true">
                          →
                        </span>
                        {edge.label}
                        {edge.lag ? (
                          <span className="mono">
                            {edge.lag.label}: {formatNumber(edge.lag.value)} {edge.lag.unit}
                          </span>
                        ) : null}
                      </div>
                    ) : null}
                  </Fragment>
                );
              })}
            </div>
            <Provenance asOf={data.as_of} source={shell.topology ? "SSE /api/stream/live" : "GET /api/topology"} />
          </section>
        )}
      </StateView>

      <section className="grid two">
        <div className="panel">
          <h2>Change events on Kafka (live)</h2>
          {shell.events.length === 0 ? (
            <p className="provenance">
              Waiting for events on lakeflow.shop.* ({shell.live}). Create an order from the Overview page.
            </p>
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Received</th>
                    <th>Topic / partition / offset</th>
                    <th>Op</th>
                    <th>Key</th>
                    <th>LSN</th>
                  </tr>
                </thead>
                <tbody>
                  {shell.events.slice(0, 15).map((e) => (
                    <tr key={`${e.topic}-${e.partition}-${e.offset}`}>
                      <td>{formatTime(new Date(e.received_at).toISOString())}</td>
                      <td className="mono">
                        {e.topic}/{e.partition}/{e.offset}
                      </td>
                      <td>{e.op ?? "tombstone"}</td>
                      <td className="mono">{e.key}</td>
                      <td className="mono">{e.lsn ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
        <div className="panel">
          <h2>Micro-batches</h2>
          <StateView {...batches} empty={(d) => d.batches.length === 0} emptyText="No micro-batches committed yet.">
            {(data) => (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Batch</th>
                      <th>Committed</th>
                      <th>Input</th>
                      <th>Applied</th>
                      <th>Dup</th>
                      <th>DLQ</th>
                      <th>p95 fresh</th>
                      <th>Files</th>
                      <th>Attempts</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.batches.map((b) => (
                      <tr key={`${b.stream_epoch}-${b.batch_id}`}>
                        <td className="mono">{b.batch_id}</td>
                        <td>{formatTime(b.committed_at)}</td>
                        <td>{b.input_rows}</td>
                        <td>{b.applied}</td>
                        <td>{b.duplicates}</td>
                        <td>{b.dlq_rows}</td>
                        <td>{b.freshness_p95_ms === null ? "—" : `${(b.freshness_p95_ms / 1000).toFixed(1)}s`}</td>
                        <td>
                          {b.added_data_files} ({formatBytes(b.added_files_bytes)})
                        </td>
                        <td>{b.attempts > 1 ? <Badge status="warning">{`${b.attempts} (replayed)`}</Badge> : b.attempts}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <Provenance asOf={data.as_of} source={data.source} />
              </div>
            )}
          </StateView>
        </div>
      </section>
    </>
  );
}
