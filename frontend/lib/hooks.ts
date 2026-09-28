"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { api, API_BASE, ApiError } from "./api";

export interface ApiState<T> {
  data?: T;
  error?: ApiError;
  loading: boolean;
  updatedAt?: number;
  stale: boolean;
  reload: () => void;
}

/** Poll an endpoint. `stale` flips on when the last success is older than 3 intervals (or 30 s). */
export function useApi<T>(path: string | null, intervalMs = 0): ApiState<T> {
  const [state, setState] = useState<{ data?: T; error?: ApiError; loading: boolean; updatedAt?: number }>({
    loading: path !== null,
  });
  const [tick, setTick] = useState(0);
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    if (!path) return;
    let cancelled = false;
    const controller = new AbortController();
    const load = async () => {
      try {
        const data = await api.get<T>(path, controller.signal);
        if (!cancelled) setState({ data, loading: false, updatedAt: Date.now() });
      } catch (error) {
        if (cancelled || (error instanceof DOMException && error.name === "AbortError")) return;
        const apiError = error instanceof ApiError ? error : new ApiError(0, String(error));
        setState((previous) => ({ ...previous, error: apiError, loading: false }));
      }
    };
    void load();
    const timer = intervalMs > 0 ? setInterval(load, intervalMs) : undefined;
    return () => {
      cancelled = true;
      controller.abort();
      if (timer) clearInterval(timer);
    };
  }, [path, intervalMs, tick]);

  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 5000);
    return () => clearInterval(timer);
  }, []);

  const reload = useCallback(() => setTick((value) => value + 1), []);
  const staleAfter = Math.max(intervalMs * 3, 30_000);
  const stale = state.updatedAt !== undefined && now - state.updatedAt > staleAfter;
  return { ...state, stale, reload };
}

export type LiveStatus = "connecting" | "live" | "offline";

/** Server-sent events from /api/stream/live (cookie-authenticated; the browser reconnects automatically). */
export function useLiveStream(handlers: Record<string, (payload: unknown) => void>): LiveStatus {
  const [status, setStatus] = useState<LiveStatus>("connecting");
  const handlersRef = useRef(handlers);

  useEffect(() => {
    handlersRef.current = handlers;
  });

  useEffect(() => {
    const source = new EventSource(`${API_BASE}/api/stream/live`, { withCredentials: true });
    source.onopen = () => setStatus("live");
    source.onerror = () => setStatus("offline");
    const kinds = ["topology", "cdc_event", "batch"];
    const listeners = kinds.map((kind) => {
      const listener = (event: MessageEvent) => {
        try {
          handlersRef.current[kind]?.(JSON.parse(event.data));
        } catch {
          // ignore malformed frames; the next topology frame resynchronises the view
        }
      };
      source.addEventListener(kind, listener as EventListener);
      return [kind, listener] as const;
    });
    return () => {
      listeners.forEach(([kind, listener]) => source.removeEventListener(kind, listener as EventListener));
      source.close();
    };
  }, []);

  return status;
}
