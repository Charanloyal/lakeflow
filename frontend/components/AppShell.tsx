"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

import { api, snapshotMeta, SNAPSHOT, type SnapshotMeta } from "@/lib/api";
import { useLiveStream, type LiveStatus } from "@/lib/hooks";
import type { Identity, Topology } from "@/lib/types";

import { Badge } from "./ui";

export interface LiveEvent {
  topic: string;
  partition: number;
  offset: number;
  key: string | null;
  op: string | null;
  lsn: number | null;
  kafka_ts_ms: number | null;
  injection_id: string | null;
  received_at: number;
}

interface LiveBatch {
  batch_id: number;
  applied: number;
  duplicates: number;
  dlq_rows: number;
  committed_at: string;
  attempts: number;
  received_at: number;
}

interface ShellContext {
  identity: Identity | null;
  live: LiveStatus;
  topology: Topology | null;
  events: LiveEvent[];
  batches: LiveBatch[];
}

const Context = createContext<ShellContext>({
  identity: null,
  live: "connecting",
  topology: null,
  events: [],
  batches: [],
});

export const useShell = () => useContext(Context);

const NAV = [
  ["/", "Overview"],
  ["/pipeline/", "Live Pipeline"],
  ["/events/", "Event Explorer"],
  ["/quality/", "Data Quality"],
  ["/lineage/", "Lineage"],
  ["/benchmarks/", "Benchmarks"],
  ["/recovery/", "Recovery Lab"],
  ["/architecture/", "Architecture & ADRs"],
] as const;

const normalize = (path: string) => path.replace(/\/+$/, "") || "/";

export function AppShell({ children }: { children: ReactNode }) {
  const pathname = normalize(usePathname() ?? "/");
  const router = useRouter();
  const [identity, setIdentity] = useState<Identity | null>(null);
  const isLogin = pathname.startsWith("/login");

  useEffect(() => {
    if (isLogin) return;
    api
      .get<Identity>("/api/auth/me")
      .then(setIdentity)
      .catch(() => router.replace("/login/"));
  }, [isLogin, router]);

  useEffect(() => {
    const onUnauthorized = () => {
      if (!window.location.pathname.startsWith("/login")) router.replace("/login/");
    };
    window.addEventListener("lakeflow:unauthorized", onUnauthorized);
    return () => window.removeEventListener("lakeflow:unauthorized", onUnauthorized);
  }, [router]);

  const logout = useCallback(async () => {
    await api.post("/api/auth/logout").catch(() => undefined);
    setIdentity(null);
    router.replace("/login/");
  }, [router]);

  if (isLogin) return <main id="main">{children}</main>;

  return (
    <div className="shell">
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      <nav className="sidebar" aria-label="Primary">
        <div className="brand">
          LakeFlow
          <small>CDC lakehouse control plane</small>
        </div>
        <ul className="nav">
          {NAV.map(([href, label]) => (
            <li key={href}>
              <Link href={href} aria-current={pathname === normalize(href) ? "page" : undefined}>
                {label}
              </Link>
            </li>
          ))}
        </ul>
      </nav>
      <div className="main">
        {identity ? (
          <Live identity={identity} logout={logout}>
            {children}
          </Live>
        ) : (
          <div className="content">Checking session…</div>
        )}
      </div>
    </div>
  );
}

function Live({
  identity,
  logout,
  children,
}: {
  identity: Identity;
  logout: () => void;
  children: ReactNode;
}) {
  const [topology, setTopology] = useState<Topology | null>(null);
  const [events, setEvents] = useState<LiveEvent[]>([]);
  const [batches, setBatches] = useState<LiveBatch[]>([]);
  const live = useLiveStream({
    topology: (payload) => setTopology(payload as Topology),
    cdc_event: (payload) =>
      setEvents((previous) =>
        [{ ...(payload as LiveEvent), received_at: Date.now() }, ...previous].slice(0, 200),
      ),
    batch: (payload) =>
      setBatches((previous) =>
        [{ ...(payload as LiveBatch), received_at: Date.now() }, ...previous].slice(0, 100),
      ),
  });
  const value = useMemo(
    () => ({ identity, live, topology, events, batches }),
    [identity, live, topology, events, batches],
  );
  const [meta, setMeta] = useState<SnapshotMeta | null>(null);
  useEffect(() => {
    snapshotMeta()
      .then(setMeta)
      .catch(() => setMeta(null));
  }, []);
  return (
    <Context.Provider value={value}>
      {SNAPSHOT ? (
        <div className="snapshot-banner" role="note" data-testid="snapshot-banner">
          <strong>Recorded demo.</strong> Every value on these pages was captured from the real stack (PostgreSQL,
          Debezium, Kafka, Spark, Iceberg, Trino) by the CI end-to-end run
          {meta ? (
            <>
              {" "}
              <a href={meta.run_url}>{meta.sha.slice(0, 7)}</a> at {meta.captured_at}
            </>
          ) : null}
          . Nothing is simulated, nothing updates, and actions are disabled.{" "}
          {meta ? (
            <>
              <a href={meta.codespaces_url}>Run it live in Codespaces</a> · <a href={meta.repo_url}>Source</a>
            </>
          ) : null}
        </div>
      ) : null}
      <header className="topbar">
        <Badge status="demo">{SNAPSHOT ? "Recorded CI snapshot" : "Local demo environment"}</Badge>
        <span className="provenance">
          Source data is generated by this demo (seed rows, your mutations, Recovery Lab injections). Nothing
          here is production data.
        </span>
        <span className="spacer" />
        <Badge status={live}>
          {live === "live" ? "Live stream connected" : live === "recorded" ? "Recorded (no live stream)" : `Live stream ${live}`}
        </Badge>
        <span className="provenance">
          {identity.user} ({identity.role})
        </span>
        {SNAPSHOT ? null : <button onClick={logout}>Sign out</button>}
      </header>
      <main id="main" className="content" tabIndex={-1}>
        {children}
      </main>
    </Context.Provider>
  );
}
