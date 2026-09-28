"use client";

import { Badge, Provenance, StateView } from "@/components/ui";
import { API_BASE } from "@/lib/api";
import { formatDateTime, formatNumber, formatPercent } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import type { BenchmarkList, Summary } from "@/lib/types";

function Latency({ summary }: { summary: Summary }) {
  return (
    <span className="mono">
      p50 {formatNumber(summary.p50, 0)} · p95 {formatNumber(summary.p95, 0)} · p99 {formatNumber(summary.p99, 0)} · σ{" "}
      {formatNumber(summary.stddev, 0)} ms
    </span>
  );
}

export default function BenchmarksPage() {
  const runs = useApi<BenchmarkList>("/api/benchmarks/runs", 60000);
  return (
    <>
      <h1>Benchmarks</h1>
      <p className="subtitle">
        Every summary is recomputed from the raw per-event samples in benchmarks/results/*.json (ADR-0005). Nothing here is
        typed in by hand.
      </p>
      <StateView
        {...runs}
        empty={(d) => d.runs.length === 0}
        emptyText={
          <>
            No benchmark results yet. Run <code>make benchmark</code> (writes raw JSON to benchmarks/results/) or trigger the CI
            evidence workflow.
          </>
        }
      >
        {(data) => (
          <>
            {data.runs.map((run) => (
              <section className="panel" key={run.run_id} aria-label={`Benchmark ${run.run_id}`}>
                <div className="row" style={{ justifyContent: "space-between" }}>
                  <h2>
                    {run.run_id} <span className="provenance">· {formatDateTime(run.created_at)} · commit {run.git_sha.slice(0, 8)}</span>
                  </h2>
                  <a className="button" href={`${API_BASE}/api/benchmarks/runs/${run.run_id}/raw`} download>
                    Download raw JSON
                  </a>
                </div>
                <div className="grid metrics">
                  <div className="metric">
                    <div className="label">End-to-end freshness</div>
                    <Latency summary={run.summary.latency_ms} />
                  </div>
                  <div className="metric">
                    <div className="label">Throughput (mean of iterations)</div>
                    <div className="value">
                      {formatNumber(run.summary.throughput_eps.mean, 1)}
                      <span className="unit">events/s ± {formatNumber(run.summary.throughput_eps.stddev, 1)}</span>
                    </div>
                  </div>
                  <div className="metric">
                    <div className="label">Loss / duplicate / error rate</div>
                    <div className="mono">
                      {formatPercent(run.summary.loss_rate)} / {formatPercent(run.summary.duplicate_rate)} /{" "}
                      {formatPercent(run.summary.error_rate)}
                    </div>
                  </div>
                  <div className="metric">
                    <div className="label">Events (observed / expected)</div>
                    <div className="value">
                      {formatNumber(run.summary.events_observed)} / {formatNumber(run.summary.events_expected)}
                    </div>
                    <div className="provenance">{run.summary.iterations} iterations · reconciliation mismatches {run.summary.reconciliation_mismatches}</div>
                  </div>
                </div>
                {run.thresholds.length ? (
                  <div className="row" style={{ marginTop: 10 }}>
                    {run.thresholds.map((t) => (
                      <Badge key={t.name} status={t.passed ? "pass" : "fail"}>
                        {`${t.name}: ${formatNumber(t.value)} (limit ${t.limit})`}
                      </Badge>
                    ))}
                  </div>
                ) : null}
                <details style={{ marginTop: 10 }}>
                  <summary>Environment, configuration and workload</summary>
                  <div className="grid two">
                    <pre>{JSON.stringify(run.environment, null, 2)}</pre>
                    <pre>{JSON.stringify({ config: run.config, workload: run.workload }, null, 2)}</pre>
                  </div>
                </details>
                <details>
                  <summary>Per-iteration results</summary>
                  <pre>{JSON.stringify(run.summary.per_iteration, null, 2)}</pre>
                </details>
                {run.summary.query_benchmarks.length ? (
                  <details>
                    <summary>Trino query benchmark (cold/warm, before/after optimize)</summary>
                    <pre>{JSON.stringify(run.summary.query_benchmarks, null, 2)}</pre>
                  </details>
                ) : null}
                <Provenance asOf={run.created_at} source={`benchmarks/results/${run.file}`} />
              </section>
            ))}
          </>
        )}
      </StateView>
    </>
  );
}
