"use client";

import { GuidedDemo } from "@/components/GuidedDemo";
import { Badge, MetricCard, Provenance, StateView } from "@/components/ui";
import { formatDateTime } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import type { ComponentsResponse, Overview } from "@/lib/types";

export default function OverviewPage() {
  const overview = useApi<Overview>("/api/metrics/overview", 5000);
  const components = useApi<ComponentsResponse>("/api/health/components", 10000);

  return (
    <>
      <div className="row" style={{ justifyContent: "space-between" }}>
        <div>
          <h1>Overview</h1>
          <p className="subtitle">Live health of the CDC path. Every number shows when it was measured and where it came from.</p>
        </div>
        {overview.data ? <Badge status={overview.data.overall}>{`platform ${overview.data.overall}`}</Badge> : null}
      </div>

      <StateView {...overview}>
        {(data) => (
          <>
            <section className="grid metrics" aria-label="Key metrics">
              {data.metrics.map((metric) => (
                <MetricCard key={metric.name} metric={metric} />
              ))}
            </section>
            <section className="grid two" style={{ marginTop: 16 }}>
              <div className="panel">
                <h2>Freshness SLA</h2>
                {data.sla.map((sla) => (
                  <div key={sla.contract} className="row" style={{ justifyContent: "space-between" }}>
                    <span>
                      <strong>{sla.contract}</strong>: p95 target {sla.p95_target_seconds}s, observed{" "}
                      {sla.p95_observed_seconds === null ? "—" : `${sla.p95_observed_seconds}s`}
                    </span>
                    <Badge status={sla.status} />
                    <Provenance asOf={sla.as_of} source={sla.source} />
                  </div>
                ))}
              </div>
              <div className="panel">
                <h2>Recent incidents</h2>
                {data.incidents.length === 0 ? (
                  <p className="provenance">No incidents recorded (health monitor + Recovery Lab actions).</p>
                ) : (
                  <ul style={{ margin: 0, paddingLeft: 18 }}>
                    {data.incidents.map((incident) => (
                      <li key={incident.id}>
                        <Badge status={incident.resolved_at ? "healthy" : incident.severity}>
                          {incident.resolved_at ? "resolved" : incident.severity}
                        </Badge>{" "}
                        {incident.title} <span className="provenance">· {formatDateTime(incident.opened_at)} · {incident.source}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </section>
          </>
        )}
      </StateView>

      <GuidedDemo />

      <section className="panel" aria-labelledby="components-title">
        <h2 id="components-title">Components</h2>
        <StateView {...components}>
          {(data) => (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Component</th>
                    <th>Status</th>
                    <th>Detail</th>
                    <th>Probe</th>
                    <th>Checked</th>
                  </tr>
                </thead>
                <tbody>
                  {data.components.map((c) => (
                    <tr key={c.component}>
                      <td>{c.label}</td>
                      <td>
                        <Badge status={c.status} />
                      </td>
                      <td>{c.detail}</td>
                      <td className="provenance">
                        {c.source}
                        {c.latency_ms ? ` · ${c.latency_ms} ms` : ""}
                      </td>
                      <td className="provenance">{formatDateTime(c.checked_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </StateView>
      </section>
    </>
  );
}
