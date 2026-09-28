"use client";

import type { ReactNode } from "react";

import { ApiError } from "@/lib/api";
import { formatNumber, formatTime } from "@/lib/format";
import type { Metric } from "@/lib/types";

export function Badge({ status, children }: { status: string; children?: ReactNode }) {
  return <span className={`badge ${status}`}>{children ?? status.replace("_", " ")}</span>;
}

export function Provenance({
  asOf,
  source,
  windowLabel,
}: {
  asOf: string | null | undefined;
  source: string;
  windowLabel?: string | null;
}) {
  return (
    <div className="provenance">
      as of {formatTime(asOf ?? null)} · {source}
      {windowLabel ? ` · window ${windowLabel}` : ""}
    </div>
  );
}

export function MetricCard({ metric }: { metric: Metric }) {
  const value = metric.value === null ? "—" : typeof metric.value === "number" ? formatNumber(metric.value) : metric.value;
  return (
    <div className="metric" data-testid={`metric-${metric.name}`}>
      <div className="row" style={{ justifyContent: "space-between" }}>
        <span className="label">{metric.label}</span>
        {metric.stale ? <Badge status="stale">stale</Badge> : null}
      </div>
      <div className="value">
        {value}
        {metric.value !== null ? <span className="unit">{metric.unit}</span> : null}
      </div>
      {metric.note ? <div className="provenance">{metric.note}</div> : null}
      <Provenance asOf={metric.as_of} source={metric.source} windowLabel={metric.window} />
    </div>
  );
}

interface StateProps<T> {
  loading: boolean;
  error?: ApiError;
  data?: T;
  stale?: boolean;
  empty?: (data: T) => boolean;
  emptyText?: ReactNode;
  children: (data: T) => ReactNode;
}

/** Renders loading, error, empty and stale states consistently around any API-backed view. */
export function StateView<T>({ loading, error, data, stale, empty, emptyText, children }: StateProps<T>) {
  if (data === undefined) {
    if (loading) return <div className="state" role="status">Loading…</div>;
    if (error)
      return (
        <div className="state error" role="alert">
          {error.status === 503 ? "Dependency unavailable: " : error.status === 0 ? "" : `Error ${error.status}: `}
          {error.message}
        </div>
      );
    return <div className="state">No data.</div>;
  }
  return (
    <>
      {error ? (
        <div className="state degraded" role="status">
          Showing the last good data; refresh failed ({error.message}).
        </div>
      ) : stale ? (
        <div className="state degraded" role="status">
          Data is stale: no successful refresh for a while.
        </div>
      ) : null}
      {empty && empty(data) ? <div className="state">{emptyText ?? "Nothing here yet."}</div> : children(data)}
    </>
  );
}

export function KeyValues({ entries }: { entries: [string, unknown][] }) {
  const shown = entries.filter(([, value]) => value !== null && value !== undefined && value !== "");
  if (!shown.length) return null;
  return (
    <dl className="kv">
      {shown.map(([key, value]) => (
        <div key={key} style={{ display: "contents" }}>
          <dt>{key}</dt>
          <dd>{typeof value === "object" ? JSON.stringify(value) : String(value)}</dd>
        </div>
      ))}
    </dl>
  );
}
