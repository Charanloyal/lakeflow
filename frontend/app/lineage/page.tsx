"use client";

import { useState } from "react";

import { Badge, StateView } from "@/components/ui";
import { formatDateTime, formatNumber } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import type { ImpactResponse, LineageResponse } from "@/lib/types";

const LAYERS = ["source", "stream", "bronze", "ops", "silver", "consumption"];

export default function LineagePage() {
  const lineage = useApi<LineageResponse>("/api/lineage", 30000);
  const [selected, setSelected] = useState<string>("postgres.shop.orders");
  const impact = useApi<ImpactResponse>(`/api/lineage/impact?dataset=${encodeURIComponent(selected)}`);
  const affected = new Set(impact.data?.affected_datasets ?? []);

  return (
    <>
      <h1>Lineage</h1>
      <p className="subtitle">
        Declared in contracts/lineage.json and enriched with live Iceberg snapshot and Kafka offset stats.
        Select a dataset for impact analysis.
      </p>
      <StateView {...lineage}>
        {(data) => (
          <>
            <section className="panel lineage" aria-label="Datasets by layer">
              {LAYERS.map((layer) => (
                <div key={layer}>
                  <h3>{layer}</h3>
                  {data.datasets
                    .filter((d) => d.layer === layer)
                    .map((d) => (
                      <button
                        key={d.id}
                        className={`dataset${d.id === selected ? " selected" : ""}${affected.has(d.id) ? " affected" : ""}`}
                        onClick={() => setSelected(d.id)}
                        aria-pressed={d.id === selected}
                      >
                        <div className="mono">{d.id}</div>
                        <div className="provenance">{d.description}</div>
                        {d.live ? (
                          <div className="provenance">
                            {formatNumber(d.live.records as number)} records
                            {d.live.last_commit ? ` · ${formatDateTime(d.live.last_commit)}` : ""} ·{" "}
                            {d.live.source}
                          </div>
                        ) : null}
                      </button>
                    ))}
                </div>
              ))}
            </section>
            <section className="grid two">
              <div className="panel">
                <h2>Impact of {selected}</h2>
                <StateView {...impact}>
                  {(result) => (
                    <>
                      <p>
                        Owners to notify:{" "}
                        {result.owners_to_notify.map((owner) => (
                          <Badge key={owner} status="info">
                            {owner}
                          </Badge>
                        ))}
                      </p>
                      <ul>
                        {result.affected.map((node) => (
                          <li key={node.id}>
                            <span className="mono">{node.id}</span>{" "}
                            <span className="provenance">
                              ({node.type}, depth {node.depth})
                            </span>
                          </li>
                        ))}
                      </ul>
                    </>
                  )}
                </StateView>
              </div>
              <div className="panel">
                <h2>Jobs</h2>
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Job</th>
                        <th>Inputs</th>
                        <th>Outputs</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.jobs.map((job) => (
                        <tr key={job.id}>
                          <td>
                            <div className="mono">{job.id}</div>
                            <div className="provenance">{job.kind}</div>
                          </td>
                          <td className="mono">{job.inputs.join(", ")}</td>
                          <td className="mono">{job.outputs.join(", ")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </section>
          </>
        )}
      </StateView>
    </>
  );
}
