"""HTTP routes. Handlers stay thin: validation is in `models`, behaviour in `services`, logic in `domain`."""

from __future__ import annotations

import asyncio
import json
import queue
from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import models as m
from .auth import SESSION_COOKIE, require_admin, require_viewer
from .domain.sessions import Identity, check_credentials, sign_session
from .services import catalog, demo, overview, quality, recovery, trace

VERSION = "2.0.0"
public = APIRouter(prefix="/api")
api = APIRouter(prefix="/api", dependencies=[Depends(require_viewer)])


def ctx_of(request: Request):
    return request.app.state.ctx


# ------------------------------------------------------------------------------------------------ public
@public.get("/health", response_model=m.HealthResponse, tags=["health"])
def health(request: Request):
    return {"status": "ok", "version": VERSION, "environment": ctx_of(request).settings.environment}


@public.get("/metrics", include_in_schema=False)
def prometheus_metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@public.post("/auth/login", response_model=m.IdentityResponse, tags=["auth"])
def login(body: m.LoginRequest, request: Request, response: Response):
    ctx = ctx_of(request)
    client = request.client.host if request.client else "unknown"
    allowed, retry = ctx.login_limiter.allow(client)
    if not allowed:
        raise HTTPException(429, f"too many login attempts; retry in {retry:.0f}s")
    identity = check_credentials(body.username, body.password, ctx.settings.users)
    if identity is None:
        raise HTTPException(401, "invalid username or password")
    response.set_cookie(SESSION_COOKIE, sign_session(identity, ctx.settings.session_secret), max_age=8 * 3600,
                        httponly=True, samesite="strict", secure=request.url.scheme == "https", path="/api")
    return {"user": identity.user, "role": identity.role}


@public.post("/auth/logout", status_code=204, tags=["auth"])
def logout(response: Response):
    response.delete_cookie(SESSION_COOKIE, path="/api")


# ------------------------------------------------------------------------------------------ authenticated
@api.get("/auth/me", response_model=m.IdentityResponse, tags=["auth"])
def me(identity: Identity = Depends(require_viewer)):
    return {"user": identity.user, "role": identity.role}


@api.get("/health/components", response_model=m.ComponentsResponse, tags=["health"])
def health_components(request: Request):
    return overview.components(ctx_of(request))


@api.get("/topology", response_model=m.TopologyResponse, tags=["pipeline"])
def get_topology(request: Request):
    return overview.topology(ctx_of(request))


@api.get("/metrics/overview", response_model=m.OverviewResponse, tags=["pipeline"])
def get_overview(request: Request):
    return overview.overview(ctx_of(request))


@api.get("/metrics/batches", response_model=m.BatchesResponse, tags=["pipeline"])
def get_batches(request: Request, limit: int = Query(50, ge=1, le=500)):
    return _trino_or_503(lambda: trace.recent_batches(ctx_of(request), limit))


@api.get("/events", response_model=m.EventsResponse, tags=["events"])
def get_events(
    request: Request,
    table: str | None = Query(None, pattern=r"^[a-z_]+$"),
    op: str | None = Query(None, pattern=r"^[cudr]$"),
    outcome: str | None = Query(None, pattern=r"^(applied|superseded|stale)$"),
    key: str | None = Query(None, max_length=64, pattern=r"^[A-Za-z0-9-]+$"),
    minutes: int = Query(60, ge=1, le=7 * 24 * 60),
    limit: int = Query(100, ge=1, le=500),
):
    return _trino_or_503(lambda: trace.list_events(ctx_of(request), table, op, outcome, key, minutes, limit))


@api.get("/events/live", tags=["events"])
def live_events(request: Request, limit: int = Query(50, ge=1, le=500)):
    ctx = ctx_of(request)
    return {"events": ctx.tracer.recent(limit), "running": ctx.tracer.running, "error": ctx.tracer.error,
            "as_of": datetime.now(timezone.utc), "source": "API Kafka tail (since API start)"}


@api.get("/trace/{table}/{key}", response_model=m.TraceResponse, tags=["events"])
def get_trace(request: Request, table: str, key: str):
    if not table.isidentifier() or len(key) > 64 or not all(c.isalnum() or c == "-" for c in key):
        raise HTTPException(422, "invalid table or key")
    return trace.trace(ctx_of(request), table, key)


# --------------------------------------------------------------------------------------------- demo (admin)
def _limit_mutations(request: Request, identity: Identity) -> None:
    allowed, retry = ctx_of(request).mutation_limiter.allow(identity.user)
    if not allowed:
        raise HTTPException(429, f"mutation rate limit; retry in {retry:.0f}s", headers={"Retry-After": str(int(retry) + 1)})


@api.post("/demo/orders", response_model=m.MutationResult, status_code=201, tags=["demo"])
def create_order(body: m.CreateOrder, request: Request, identity: Identity = Depends(require_admin)):
    _limit_mutations(request, identity)
    return demo.create_order(ctx_of(request), body, identity.user)


@api.patch("/demo/orders/{order_id}", response_model=m.MutationResult, tags=["demo"])
def update_order(order_id: UUID, body: m.UpdateOrder, request: Request, identity: Identity = Depends(require_admin)):
    _limit_mutations(request, identity)
    return demo.update_order(ctx_of(request), order_id, body, identity.user)


@api.delete("/demo/orders/{order_id}", response_model=m.MutationResult, tags=["demo"])
def delete_order(order_id: UUID, request: Request, identity: Identity = Depends(require_admin)):
    _limit_mutations(request, identity)
    return demo.delete_order(ctx_of(request), order_id, identity.user)


@api.post("/demo/mutations", response_model=m.MutationBatchResult, tags=["demo"])
def generate_mutations(body: m.MutationBatch, request: Request, identity: Identity = Depends(require_admin)):
    _limit_mutations(request, identity)
    return demo.generate(ctx_of(request), body.count, body.seed, identity.user)


# --------------------------------------------------------------------------------------------------- quality
@api.get("/quality/summary", response_model=m.QualitySummary, tags=["quality"])
def quality_summary(request: Request):
    return quality.summary(ctx_of(request))


@api.post("/quality/run", response_model=m.QualityRunResponse, tags=["quality"])
def quality_run(body: m.QualityRunRequest, request: Request, identity: Identity = Depends(require_admin)):
    return quality.run(ctx_of(request), body.checks, runner=f"api:{identity.user}")


@api.get("/quality/rejected", response_model=m.DlqResponse, tags=["quality"])
def quality_rejected(request: Request, status: str | None = Query(None, pattern=r"^(open|replayed)$"),
                     limit: int = Query(100, ge=1, le=500)):
    return _trino_or_503(lambda: quality.rejected(ctx_of(request), status, limit))


@api.post("/quality/dlq/replay", response_model=m.ReplayResponse, tags=["quality"])
def quality_replay(body: m.ReplayRequest, request: Request, identity: Identity = Depends(require_admin)):
    if any(len(i) != 64 or not all(c in "0123456789abcdef" for c in i) for i in body.dlq_ids):
        raise HTTPException(422, "dlq_ids must be 64-character hex ids")
    return quality.replay(ctx_of(request), body.dlq_ids, identity.user)


@api.get("/quality/schema", response_model=m.SchemaResponse, tags=["quality"])
def quality_schema(request: Request):
    return quality.schema(ctx_of(request))


@api.get("/quality/freshness", response_model=m.FreshnessResponse, tags=["quality"])
def quality_freshness(request: Request, minutes: int = Query(60, ge=5, le=24 * 60)):
    return _trino_or_503(lambda: quality.freshness(ctx_of(request), minutes))


# ------------------------------------------------------------------------------------------ lineage / docs
@api.get("/lineage", response_model=m.LineageResponse, tags=["lineage"])
def get_lineage(request: Request):
    return catalog.lineage_graph(ctx_of(request))


@api.get("/lineage/impact", response_model=m.ImpactResponse, tags=["lineage"])
def get_impact(request: Request, dataset: str = Query(..., max_length=200, pattern=r"^[A-Za-z0-9_.\-]+$")):
    return catalog.impact(ctx_of(request), dataset)


@api.get("/benchmarks/runs", response_model=m.BenchmarkList, tags=["benchmarks"])
def benchmark_runs(request: Request):
    return catalog.benchmark_runs(ctx_of(request))


@api.get("/benchmarks/runs/{run_id}/raw", tags=["benchmarks"])
def benchmark_raw(request: Request, run_id: str):
    if len(run_id) > 80 or not all(c.isalnum() or c in "-_" for c in run_id):
        raise HTTPException(422, "invalid run id")
    path = catalog.benchmark_file(ctx_of(request), run_id)
    return FileResponse(path, media_type="application/json", filename=path.name)


@api.get("/architecture/adrs", response_model=m.AdrList, tags=["architecture"])
def architecture_adrs(request: Request):
    return catalog.adrs(ctx_of(request))


# ----------------------------------------------------------------------------------------------- recovery
@api.get("/recovery/actions", response_model=m.RecoveryList, tags=["recovery"])
def recovery_history(request: Request):
    return recovery.history(ctx_of(request))


@api.post("/recovery/actions", response_model=m.RecoveryAction, status_code=202, tags=["recovery"])
def recovery_run(body: m.RecoveryRequest, request: Request, identity: Identity = Depends(require_admin)):
    return recovery.run(ctx_of(request), body, identity.user)


# ---------------------------------------------------------------------------------------------------- SSE
@api.get("/stream/live", tags=["pipeline"])
async def stream_live(request: Request):
    """Server-sent events: `cdc_event` and `batch` as they happen, `topology` every 5 s."""
    ctx = ctx_of(request)
    try:
        q = ctx.broadcaster.subscribe()
    except RuntimeError as exc:
        raise HTTPException(503, str(exc)) from exc

    async def events():
        last_topology = 0.0
        try:
            yield "retry: 3000\n\n"
            while not await request.is_disconnected():
                loop_time = asyncio.get_running_loop().time()
                if loop_time - last_topology >= 5:
                    last_topology = loop_time
                    payload = await asyncio.to_thread(overview.topology, ctx)
                    yield f"event: topology\ndata: {json.dumps(payload, default=str)}\n\n"
                drained = 0
                while drained < 100:
                    try:
                        kind, payload = q.get_nowait()
                    except queue.Empty:
                        break
                    drained += 1
                    yield f"event: {kind}\ndata: {json.dumps(payload, default=str)}\n\n"
                await asyncio.sleep(0.25)
        finally:
            ctx.broadcaster.unsubscribe(q)

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})


def _trino_or_503(fn):
    try:
        return fn()
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 - dependency down => explicit 503 with the cause
        raise HTTPException(503, f"lakehouse query failed: {type(exc).__name__}: {str(exc)[:200]}") from exc
