"""Typed request/response models (they generate the OpenAPI document at /api/openapi.json)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

Status = Literal["healthy", "degraded", "down", "unknown"]
OrderStatus = Literal["PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]
Currency = Literal["USD", "EUR", "GBP", "INR"]
Channel = Literal["web", "mobile", "store"]


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Metric(BaseModel):
    """Every number the UI shows carries its source and timestamp; `stale`/`note` explain gaps."""

    name: str
    label: str
    value: float | int | str | None
    unit: str
    as_of: datetime | None
    source: str
    window: str | None = None
    stale: bool = False
    note: str | None = None


class ComponentHealth(BaseModel):
    component: str
    label: str
    status: Status
    checked_at: datetime | None
    latency_ms: float | None = None
    detail: str
    source: str
    data: dict[str, Any] = {}


class HealthResponse(BaseModel):
    status: Literal["ok"]
    version: str
    environment: str


class ComponentsResponse(BaseModel):
    overall: Status
    components: list[ComponentHealth]
    as_of: datetime


class Incident(BaseModel):
    id: int
    opened_at: datetime
    resolved_at: datetime | None
    component: str
    severity: str
    title: str
    detail: str | None
    source: str


class SlaStatus(BaseModel):
    contract: str
    p95_target_seconds: float
    p95_observed_seconds: float | None
    status: Literal["met", "breached", "no_data"]
    as_of: datetime | None
    source: str


class OverviewResponse(BaseModel):
    environment: str
    overall: Status
    metrics: list[Metric]
    sla: list[SlaStatus]
    incidents: list[Incident]
    as_of: datetime


class TopologyNode(BaseModel):
    id: str
    label: str
    status: Status
    detail: str
    metrics: list[Metric]


class TopologyEdge(BaseModel):
    source: str
    target: str
    label: str
    lag: Metric | None = None


class TopologyResponse(BaseModel):
    nodes: list[TopologyNode]
    edges: list[TopologyEdge]
    as_of: datetime


class TraceStage(BaseModel):
    stage: str
    label: str
    status: Literal["done", "pending", "failed", "skipped"]
    at: str | None
    details: dict[str, Any]


class TraceResponse(BaseModel):
    table: str
    key: str
    stages: list[TraceStage]
    events: list[dict[str, Any]]
    rejected: list[dict[str, Any]]
    final_state: dict[str, Any] | None
    source_state: dict[str, Any] | None
    freshness_ms: int | None
    schema_version: int | None
    as_of: str
    sources: list[str]


class EventsResponse(BaseModel):
    events: list[dict[str, Any]]
    count: int
    as_of: datetime
    source: str


class BatchesResponse(BaseModel):
    batches: list[dict[str, Any]]
    as_of: datetime
    source: str


class CreateOrder(Strict):
    customer_id: UUID | None = None
    amount: Decimal = Field(default=Decimal("25.00"), ge=0, le=Decimal("1000000"), max_digits=12, decimal_places=2)
    currency: Currency = "USD"
    status: OrderStatus = "PENDING"
    channel: Channel | None = None


class UpdateOrder(Strict):
    status: OrderStatus | None = None
    amount: Decimal | None = Field(default=None, ge=0, le=Decimal("1000000"), max_digits=12, decimal_places=2)
    currency: Currency | None = None
    channel: Channel | None = None

    @model_validator(mode="after")
    def at_least_one(self):
        if not self.model_fields_set:
            raise ValueError("provide at least one field to update")
        return self


class MutationResult(BaseModel):
    order_id: str
    op: Literal["c", "u", "d"]
    txid: int | None
    commit_lsn: str | None
    committed_at: datetime
    note: str


class MutationBatch(Strict):
    count: int = Field(default=20, ge=1, le=500)
    seed: int = Field(default=42, ge=0, le=2**31 - 1)


class MutationBatchResult(BaseModel):
    seed: int
    inserted: int
    updated: int
    deleted: int
    order_ids: list[str]
    elapsed_ms: float


class LoginRequest(Strict):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=256)


class IdentityResponse(BaseModel):
    user: str
    role: Literal["viewer", "admin"]


class QualityRunRequest(Strict):
    checks: list[str] | None = Field(default=None, max_length=50)


class QualityResult(BaseModel):
    check_id: str
    dataset: str
    category: str
    severity: str
    description: str
    value: float | None
    threshold: float
    unit: str
    status: Literal["pass", "fail", "error", "no_data"]
    error: str | None
    run_at: datetime | None = None


class QualityRunResponse(BaseModel):
    results: list[QualityResult]
    run_at: datetime
    source: str
    persisted: bool


class QualitySummary(BaseModel):
    results: list[QualityResult]
    dlq_open: int | None
    as_of: datetime
    source: str


class DlqResponse(BaseModel):
    records: list[dict[str, Any]]
    as_of: datetime
    source: str


class ReplayRequest(Strict):
    dlq_ids: list[str] = Field(min_length=1, max_length=50)


class ReplayResponse(BaseModel):
    replayed: list[str]
    missing: list[str]
    topic: str


class SchemaResponse(BaseModel):
    contracts: list[dict[str, Any]]
    observed_versions: list[dict[str, Any]]
    drift: list[dict[str, Any]]
    as_of: datetime
    source: str


class FreshnessResponse(BaseModel):
    points: list[dict[str, Any]]
    sla_p95_seconds: float
    as_of: datetime
    source: str


class LineageResponse(BaseModel):
    datasets: list[dict[str, Any]]
    jobs: list[dict[str, Any]]
    as_of: datetime


class ImpactResponse(BaseModel):
    dataset: str
    affected: list[dict[str, Any]]
    affected_datasets: list[str]
    affected_jobs: list[str]
    owners_to_notify: list[str]


class BenchmarkRun(BaseModel):
    run_id: str
    file: str
    created_at: str
    git_sha: str
    profile: str | None
    environment: dict[str, Any]
    config: dict[str, Any]
    workload: dict[str, Any]
    summary: dict[str, Any]
    thresholds: list[dict[str, Any]]


class BenchmarkList(BaseModel):
    runs: list[BenchmarkRun]
    source: str


RecoveryActionName = Literal[
    "inject_duplicates",
    "inject_malformed",
    "inject_late",
    "crash_now",
    "crash_after_commit",
    "restart_connector",
    "apply_schema_migration",
]


class RecoveryRequest(Strict):
    action: RecoveryActionName
    count: int = Field(default=3, ge=1, le=20)
    kind: Literal["invalid_json", "contract_violation", "unsupported_op", "key_mismatch"] = "contract_violation"


class RecoveryAction(BaseModel):
    id: str
    action: str
    actor: str
    requested_at: datetime
    status: Literal["done", "requested", "failed"]
    detail: dict[str, Any]


class RecoveryList(BaseModel):
    enabled: bool
    actions: list[RecoveryAction]
    acks: list[dict[str, Any]]


class AdrList(BaseModel):
    adrs: list[dict[str, Any]]
