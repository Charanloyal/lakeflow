"use client";

import { formatTime } from "@/lib/format";
import type { Trace } from "@/lib/types";

import { Badge, KeyValues } from "./ui";

const ICON: Record<string, string> = { done: "✓", pending: "…", failed: "✕", skipped: "–" };

/** Seven-stage record timeline; the first non-done stage is marked current (aria-live announces progress). */
export function Timeline({ trace }: { trace: Trace }) {
  const current = trace.stages.find((stage) => stage.status === "pending")?.stage;
  const doneCount = trace.stages.filter((stage) => stage.status === "done").length;
  return (
    <div>
      <p className="visually-hidden" aria-live="polite">
        {doneCount} of {trace.stages.length} stages complete
        {current ? `, waiting for ${current}` : ""}
      </p>
      <ol className="timeline" aria-label={`Trace for ${trace.table} ${trace.key}`}>
        {trace.stages.map((stage) => (
          <li
            key={stage.stage}
            className={stage.stage === current ? "current" : undefined}
            data-testid={`stage-${stage.stage}`}
          >
            <div className="row" style={{ justifyContent: "space-between" }}>
              <strong>{stage.label}</strong>
              <Badge status={stage.status}>
                <span aria-hidden="true">{ICON[stage.status]}</span> {stage.status}
              </Badge>
            </div>
            <div className="provenance">{stage.at ? formatTime(stage.at) : "—"}</div>
            <KeyValues entries={Object.entries(stage.details)} />
          </li>
        ))}
      </ol>
      <div className="row" style={{ marginTop: 8 }}>
        <span className="provenance">
          freshness {trace.freshness_ms === null ? "—" : `${(trace.freshness_ms / 1000).toFixed(1)} s`} ·
          schema version {trace.schema_version ?? "—"} · sources: {trace.sources.join("; ")}
        </span>
      </div>
    </div>
  );
}
