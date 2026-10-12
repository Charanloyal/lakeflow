// Provenance of the recorded demo: CI writes snapshot/meta.json next to the captured API responses.
// Pure functions only (no React, no fetch) so `node --test` can exercise them without a browser or a build.

export interface SnapshotMeta {
  sha: string;
  captured_at: string;
  run_url: string;
  repo_url: string;
  codespaces_url: string;
}

/** ok: valid and recent; stale: valid but older than the threshold; missing/malformed: provenance unknown. */
export type ProvenanceStatus = "ok" | "stale" | "missing" | "malformed";

export interface SnapshotProvenance {
  status: ProvenanceStatus;
  meta: SnapshotMeta | null;
  ageDays: number | null;
  problems: string[];
}

/**
 * Assumption, not a documented policy: a recording older than this is flagged as stale. Override at build time
 * with NEXT_PUBLIC_SNAPSHOT_STALE_AFTER_DAYS. The capture age is always shown, whatever the threshold.
 */
export const DEFAULT_STALE_AFTER_DAYS = 30;

const DAY_MS = 86_400_000;
const COMMIT_SHA = /^[0-9a-f]{7,40}$/;
const URL_KEYS = ["run_url", "repo_url", "codespaces_url"] as const;

export function staleAfterDays(value: string | undefined): number {
  const parsed = Number(value);
  return value !== undefined && value !== "" && Number.isFinite(parsed) && parsed > 0
    ? parsed
    : DEFAULT_STALE_AFTER_DAYS;
}

function isHttpsUrl(value: unknown): value is string {
  if (typeof value !== "string") return false;
  try {
    return new URL(value).protocol === "https:";
  } catch {
    return false;
  }
}

function unknown(status: "missing" | "malformed", problems: string[]): SnapshotProvenance {
  return { status, meta: null, ageDays: null, problems };
}

/** Validate meta.json text (null when the file is absent). Links are only accepted as https URLs. */
export function parseSnapshotMeta(
  text: string | null,
  nowMs: number,
  staleAfter: number = DEFAULT_STALE_AFTER_DAYS,
): SnapshotProvenance {
  if (text === null) return unknown("missing", ["snapshot/meta.json was not found"]);
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    return unknown("malformed", ["snapshot/meta.json is not valid JSON"]);
  }
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return unknown("malformed", ["snapshot/meta.json is not a JSON object"]);
  }
  const doc = raw as Record<string, unknown>;
  const problems: string[] = [];
  if (typeof doc.sha !== "string" || !COMMIT_SHA.test(doc.sha)) problems.push("sha is not a git commit id");
  const captured = typeof doc.captured_at === "string" ? Date.parse(doc.captured_at) : Number.NaN;
  if (Number.isNaN(captured)) problems.push("captured_at is not a timestamp");
  else if (captured - nowMs > DAY_MS) problems.push("captured_at is in the future");
  for (const key of URL_KEYS) {
    if (!isHttpsUrl(doc[key])) problems.push(`${key} is not an https URL`);
  }
  if (doc.mode !== undefined && doc.mode !== "recorded") problems.push('mode is not "recorded"');
  if (problems.length > 0) return unknown("malformed", problems);

  const ageDays = Math.max(0, (nowMs - captured) / DAY_MS);
  const meta: SnapshotMeta = {
    sha: String(doc.sha),
    captured_at: String(doc.captured_at),
    run_url: String(doc.run_url),
    repo_url: String(doc.repo_url),
    codespaces_url: String(doc.codespaces_url),
  };
  return { status: ageDays > staleAfter ? "stale" : "ok", meta, ageDays, problems: [] };
}

export function describeAge(ageDays: number): string {
  if (ageDays < 1) {
    const hours = Math.floor(ageDays * 24);
    return hours < 1 ? "less than an hour ago" : `${hours} h ago`;
  }
  const days = Math.floor(ageDays);
  return `${days} day${days === 1 ? "" : "s"} ago`;
}
