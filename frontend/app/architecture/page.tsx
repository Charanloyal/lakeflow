"use client";

import { Markdown } from "@/components/Markdown";
import { Badge, StateView } from "@/components/ui";
import { useApi } from "@/lib/hooks";
import type { Adr } from "@/lib/types";

const LAYERS = [
  ["PostgreSQL 16", "wal_level=logical · pgoutput slot · heartbeat table · REPLICA IDENTITY FULL"],
  ["Debezium 2.7", "Kafka Connect · decimal=string · tombstones · signal table (incremental snapshots)"],
  ["Kafka 3.9 (KRaft)", "key = primary key · 3 partitions · 7 d retention · topics as code"],
  ["Spark 3.5", "foreachBatch · contract UDF · dedup · LSN-guarded MERGE · checkpoint · DLQ"],
  ["Iceberg + MinIO", "REST catalog (PostgreSQL) · bronze/silver/ops · merge-on-read · snapshots"],
  ["Trino 476", "analytics · reconciliation with PostgreSQL · maintenance procedures"],
];

export default function ArchitecturePage() {
  const adrs = useApi<{ adrs: Adr[] }>("/api/architecture/adrs");
  return (
    <>
      <h1>Architecture and decisions</h1>
      <p className="subtitle">
        The primary path, then the design records that explain each trade-off (served from docs/adr).
      </p>
      <section className="panel" aria-label="Primary path">
        <div className="pipeline">
          {LAYERS.map(([title, detail], index) => (
            <div key={title} className="row" style={{ flexWrap: "nowrap", alignItems: "stretch" }}>
              <div className="stage-node">
                <h3>{title}</h3>
                <p className="provenance">{detail}</p>
              </div>
              {index < LAYERS.length - 1 ? (
                <div className="connector" aria-hidden="true">
                  <span className="arrow">→</span>
                </div>
              ) : null}
            </div>
          ))}
        </div>
        <p className="provenance">
          Control plane: FastAPI (typed endpoints, session auth, SSE) + this Next.js UI behind nginx.
          Operations: Airflow (compaction, snapshot expiry, validation, backfill), Prometheus + Grafana.
        </p>
      </section>
      <StateView {...adrs}>
        {(data) => (
          <>
            {data.adrs.map((adr) => (
              <article className="panel" key={adr.id} aria-labelledby={adr.id}>
                <div className="row" style={{ justifyContent: "space-between" }}>
                  <h2 id={adr.id}>
                    {adr.id}: {adr.title}
                  </h2>
                  <span className="row">
                    <Badge status="done">{adr.status ?? "unknown"}</Badge>
                    <span className="provenance">{adr.date}</span>
                  </span>
                </div>
                {Object.entries(adr.sections).map(([heading, body]) => (
                  <details key={heading} open={heading === "Decision"}>
                    <summary>
                      <strong>{heading}</strong>
                    </summary>
                    <Markdown text={body} />
                  </details>
                ))}
              </article>
            ))}
          </>
        )}
      </StateView>
    </>
  );
}
