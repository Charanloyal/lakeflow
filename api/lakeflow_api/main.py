"""FastAPI application factory: `uvicorn lakeflow_api.main:app`."""

from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager

import httpx
import psycopg
from confluent_kafka import KafkaException
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from trino.exceptions import TrinoConnectionError, TrinoQueryError

from .context import build_context
from .routes import VERSION, api, public
from .settings import Settings

log = logging.getLogger("lakeflow.api")


def create_app(settings: Settings | None = None, context=None) -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = settings or Settings.from_env()
    ctx = context or build_context(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if settings.start_background:
            try:
                ctx.store.init()
            except Exception:  # noqa: BLE001 - start degraded; incidents are retried later
                log.exception("control database unavailable at startup")
            ctx.tracer.start()
            ctx.monitor.start()
        yield

    app = FastAPI(
        title="LakeFlow control plane",
        version=VERSION,
        description="Typed API for the LakeFlow CDC lakehouse. Every metric carries its source and timestamp.",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.ctx = ctx
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(settings.cors_origins),
            allow_credentials=True,
            allow_methods=["GET", "POST", "PATCH", "DELETE"],
            allow_headers=["Content-Type", "Authorization", "X-LakeFlow-CSRF"],
        )

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        if request.url.path.startswith("/api/") and not request.url.path.startswith("/api/docs"):
            response.headers.setdefault("Cache-Control", "no-store")
        return response

    app.include_router(public)
    app.include_router(api)

    dependency_errors = {
        psycopg.OperationalError: "PostgreSQL",
        KafkaException: "Kafka",
        httpx.HTTPError: "an HTTP dependency",
        TrinoConnectionError: "Trino",
        TrinoQueryError: "Trino",
    }
    for error_type, name in dependency_errors.items():
        app.add_exception_handler(error_type, _dependency_handler(name))

    @app.exception_handler(Exception)
    async def unexpected(request: Request, exc: Exception):
        error_id = uuid.uuid4().hex[:10]
        log.exception("unhandled error %s on %s %s", error_id, request.method, request.url.path)
        return JSONResponse(status_code=500, content={"detail": "internal error", "error_id": error_id})

    return app


def _dependency_handler(name: str):
    async def handler(request: Request, exc: Exception):
        return JSONResponse(status_code=503, content={"detail": f"{name} unavailable: {str(exc)[:300]}"})

    return handler
