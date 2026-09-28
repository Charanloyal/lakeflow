// Types mirror api/lakeflow_api/models.py (the OpenAPI document at /api/openapi.json is the source of truth).

export type Status = "healthy" | "degraded" | "down" | "unknown";

export interface Metric {
  name: string;
  label: string;
  value: number | string | null;
  unit: string;
  as_of: string | null;
  source: string;
  window?: string | null;
  stale: boolean;
  note?: string | null;
}

export interface ComponentHealth {
  component: string;
  label: string;
  status: Status;
  checked_at: string | null;
  latency_ms?: number | null;
  detail: string;
  source: string;
  data: Record<string, unknown>;
}

export interface Incident {
  id: number;
  opened_at: string;
  resolved_at: string | null;
  component: string;
  severity: string;
  title: string;
  detail: string | null;
  source: string;
}

export interface SlaStatus {
  contract: string;
  p95_target_seconds: number;
  p95_observed_seconds: number | null;
  status: "met" | "breached" | "no_data";
  as_of: string | null;
  source: string;
}

export interface Overview {
  environment: string;
  overall: Status;
  metrics: Metric[];
  sla: SlaStatus[];
  incidents: Incident[];
  as_of: string;
}

export interface ComponentsResponse {
  overall: Status;
  components: ComponentHealth[];
  as_of: string;
}

export interface TopologyNode {
  id: string;
  label: string;
  status: Status;
  detail: string;
  metrics: Metric[];
}

export interface TopologyEdge {
  source: string;
  target: string;
  label: string;
  lag: Metric | null;
}

export interface Topology {
  nodes: TopologyNode[];
  edges: TopologyEdge[];
  as_of: string;
}

export type StageStatus = "done" | "pending" | "failed" | "skipped";

export interface TraceStage {
  stage: string;
  label: string;
  status: StageStatus;
  at: string | null;
  details: Record<string, unknown>;
}

export interface TraceEvent {
  event_id: string | null;
  op: string | null;
  source_lsn: number;
  lsn: string;
  tx_id: number | null;
  source_ts: string | null;
  kafka_topic: string | null;
  kafka_partition: number | null;
  kafka_offset: number | null;
  batch_id: number | null;
  apply_outcome: string | null;
  is_late: boolean | null;
  contract_version: number | null;
  before: Record<string, unknown> | null;
  after: Record<string, unknown> | null;
  committed_at: string | null;
  snapshot_id: number | null;
  injection_id: string | null;
}

export interface Trace {
  table: string;
  key: string;
  stages: TraceStage[];
  events: TraceEvent[];
  rejected: Record<string, unknown>[];
  final_state: Record<string, unknown> | null;
  source_state: Record<string, unknown> | null;
  freshness_ms: number | null;
  schema_version: number | null;
  as_of: string;
  sources: string[];
}

export interface BronzeEvent {
  event_id: string;
  source_table: string;
  primary_key: string;
  op: string;
  source_lsn: number;
  source_ts: string;
  kafka_topic: string;
  kafka_partition: number;
  kafka_offset: number;
  batch_id: number;
  apply_outcome: string;
  is_late: boolean;
  contract_version: number;
  drift_fields: string[] | null;
  injection_id: string | null;
  committed_at: string;
}

export interface EventsResponse {
  events: BronzeEvent[];
  count: number;
  as_of: string;
  source: string;
}

export interface BatchRecord {
  batch_id: number;
  stream_epoch: string;
  attempts: number;
  committed_at: string;
  duration_ms: number;
  input_rows: number;
  valid_rows: number;
  dlq_rows: number;
  duplicates: number;
  applied: number;
  superseded: number;
  stale: number;
  late_rows: number;
  commit_retries: number;
  freshness_p95_ms: number | null;
  added_data_files: number;
  added_files_bytes: number;
}

export interface BatchesResponse {
  batches: BatchRecord[];
  as_of: string;
  source: string;
}

export interface MutationResult {
  order_id: string;
  op: "c" | "u" | "d";
  txid: number | null;
  commit_lsn: string | null;
  committed_at: string;
  note: string;
}

export interface QualityResult {
  check_id: string;
  dataset: string;
  category: string;
  severity: string;
  description: string;
  value: number | null;
  threshold: number;
  unit: string;
  status: "pass" | "fail" | "error" | "no_data";
  error: string | null;
  run_at?: string | null;
}

export interface QualitySummary {
  results: QualityResult[];
  dlq_open: number | null;
  as_of: string;
  source: string;
}

export interface QualityRun {
  results: QualityResult[];
  run_at: string;
  source: string;
  persisted: boolean;
}

export interface DlqRecord {
  dlq_id: string;
  first_seen_at: string;
  kafka_topic: string;
  kafka_partition: number;
  kafka_offset: number;
  source_topic: string;
  error_code: string;
  error_detail: string;
  violations: string[] | null;
  injection_id: string | null;
  status: string;
  replay_attempts: number;
  raw_value: string | null;
}

export interface DlqResponse {
  records: DlqRecord[];
  as_of: string;
  source: string;
}

export interface ContractField {
  name: string;
  type: string;
  nullable: boolean;
  pii: string;
  pii_handling: string;
  enum: string[] | null;
}

export interface ContractDescription {
  contract: string;
  version: number;
  status: string;
  owner: Record<string, string>;
  classification: string;
  compatibility: string;
  freshness_sla: Record<string, unknown>;
  required: string[];
  fields: ContractField[];
  compatibility_problems: string[];
}

export interface SchemaResponse {
  contracts: ContractDescription[];
  observed_versions: { contract_name: string; contract_version: number; events: number; last_seen: string }[];
  drift: { contract_name: string; field: string; events: number; last_seen: string }[];
  as_of: string;
  source: string;
}

export interface FreshnessPoint {
  batch_id: number;
  committed_at: string;
  freshness_p50_ms: number | null;
  freshness_p95_ms: number | null;
  freshness_max_ms: number | null;
  applied: number;
  input_rows: number;
  duration_ms: number;
}

export interface FreshnessResponse {
  points: FreshnessPoint[];
  sla_p95_seconds: number;
  as_of: string;
  source: string;
}

export interface Dataset {
  id: string;
  layer: string;
  owner: string;
  description: string;
  contract?: string;
  live?: { records?: number | string | null; last_commit?: string | null; source: string } | null;
}

export interface Job {
  id: string;
  kind: string;
  owner: string;
  inputs: string[];
  outputs: string[];
}

export interface LineageResponse {
  datasets: Dataset[];
  jobs: Job[];
  as_of: string;
}

export interface ImpactResponse {
  dataset: string;
  affected: { id: string; type: string; depth: number; owner?: string }[];
  affected_datasets: string[];
  affected_jobs: string[];
  owners_to_notify: string[];
}

export interface Summary {
  count: number;
  min: number | null;
  max: number | null;
  mean: number | null;
  stddev: number | null;
  p50: number | null;
  p95: number | null;
  p99: number | null;
}

export interface BenchmarkRun {
  run_id: string;
  file: string;
  created_at: string;
  git_sha: string;
  profile: string | null;
  environment: Record<string, unknown>;
  config: Record<string, unknown>;
  workload: Record<string, unknown>;
  summary: {
    iterations: number;
    events_expected: number;
    events_observed: number;
    latency_ms: Summary;
    throughput_eps: Summary;
    duplicate_rate: number;
    loss_rate: number;
    error_rate: number;
    reconciliation_mismatches: number;
    per_iteration: Record<string, unknown>[];
    query_benchmarks: Record<string, unknown>[];
  };
  thresholds: { name: string; metric: string; value: number | null; limit: number; passed: boolean }[];
}

export interface BenchmarkList {
  runs: BenchmarkRun[];
  source: string;
}

export interface RecoveryAction {
  id: string;
  action: string;
  actor: string;
  requested_at: string;
  status: "done" | "requested" | "failed";
  detail: Record<string, unknown>;
}

export interface RecoveryList {
  enabled: boolean;
  actions: RecoveryAction[];
  acks: Record<string, unknown>[];
}

export interface Adr {
  id: string;
  title: string;
  status: string | null;
  date: string | null;
  file: string;
  sections: Record<string, string>;
}

export interface Identity {
  user: string;
  role: "viewer" | "admin";
}
