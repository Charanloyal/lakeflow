// Typed fetch wrapper. Session cookie auth (HttpOnly, SameSite=Strict); writes add the CSRF header.

export const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "";
export const BASE_PATH = process.env.NEXT_PUBLIC_BASE_PATH ?? "";
/** Recorded demo (GitHub Pages): GET responses captured from the real stack in CI; writes are disabled. */
export const SNAPSHOT = process.env.NEXT_PUBLIC_SNAPSHOT === "1";
export const READ_ONLY_MESSAGE = "Recorded demo is read-only. Run the live stack (Codespaces or make up) to change data.";

export interface SnapshotMeta {
  sha: string;
  captured_at: string;
  run_url: string;
  repo_url: string;
  codespaces_url: string;
}

let manifest: Promise<Record<string, string>> | undefined;

async function snapshotGet<T>(path: string): Promise<T> {
  if (path === "/api/auth/me") return { user: "recorded-demo", role: "viewer" } as T;
  manifest ??= fetch(`${BASE_PATH}/snapshot/manifest.json`).then((r) => r.json() as Promise<Record<string, string>>);
  const files = await manifest;
  const file = files[path] ?? files[path.split("?")[0]];
  if (!file) throw new ApiError(404, "Not captured in this recorded snapshot; run the live stack to explore it.");
  const response = await fetch(`${BASE_PATH}/snapshot/${file}`);
  if (!response.ok) throw new ApiError(response.status, "Snapshot file missing");
  return (await response.json()) as T;
}

export async function snapshotMeta(): Promise<SnapshotMeta | null> {
  if (!SNAPSHOT) return null;
  const response = await fetch(`${BASE_PATH}/snapshot/meta.json`);
  return response.ok ? ((await response.json()) as SnapshotMeta) : null;
}

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
  if (SNAPSHOT) {
    if (method === "GET") return snapshotGet<T>(path);
    throw new ApiError(405, READ_ONLY_MESSAGE);
  }
  const headers: Record<string, string> = { Accept: "application/json" };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  if (method !== "GET") headers["X-LakeFlow-CSRF"] = "1";
  let response: Response;
  try {
    response = await fetch(`${API_BASE}${path}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      credentials: "include",
      cache: "no-store",
      signal,
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") throw error;
    throw new ApiError(0, "API unreachable (is the stack up?)");
  }
  if (response.status === 401 && typeof window !== "undefined") {
    window.dispatchEvent(new CustomEvent("lakeflow:unauthorized"));
  }
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const payload = await response.json();
      detail = typeof payload.detail === "string" ? payload.detail : JSON.stringify(payload.detail);
    } catch {
      // non-JSON error body: keep the status text
    }
    throw new ApiError(response.status, detail);
  }
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

export const api = {
  get: <T>(path: string, signal?: AbortSignal) => request<T>("GET", path, undefined, signal),
  post: <T>(path: string, body?: unknown) => request<T>("POST", path, body ?? {}),
  patch: <T>(path: string, body: unknown) => request<T>("PATCH", path, body),
  del: <T>(path: string) => request<T>("DELETE", path),
};
