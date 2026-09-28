"""Prometheus metrics exposed by the Spark driver on :METRICS_PORT/metrics."""

from __future__ import annotations

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except ImportError:  # tests may run without the exporter installed
    Counter = Gauge = Histogram = start_http_server = None


class _Noop:
    def labels(self, *args, **kwargs):
        return self

    def inc(self, *args, **kwargs):
        pass

    def set(self, *args, **kwargs):
        pass

    def observe(self, *args, **kwargs):
        pass


class PipelineMetrics:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled and Counter is not None
        if not self.enabled:
            noop = _Noop()
            for name in ("events", "batches", "retries", "batch_seconds", "last_batch_id", "last_batch_ts",
                         "freshness_p95", "freshness_p50", "freshness_max", "query_active", "watermark", "file_bytes",
                         "contract_reloads", "input_rows_per_second", "processed_rows_per_second", "files_written"):
                setattr(self, name, noop)
            return
        self.events = Counter("lakeflow_stream_events_total", "Change events by table and outcome", ["table", "outcome"])
        self.batches = Counter("lakeflow_stream_batches_total", "Micro-batches by result", ["result"])
        self.retries = Counter("lakeflow_stream_commit_retries_total", "Iceberg commit retries", ["stage"])
        self.batch_seconds = Histogram(
            "lakeflow_stream_batch_duration_seconds",
            "Wall time of foreachBatch",
            buckets=(0.5, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89),
        )
        self.last_batch_id = Gauge("lakeflow_stream_last_batch_id", "Last committed micro-batch id")
        self.last_batch_ts = Gauge("lakeflow_stream_last_batch_timestamp_seconds", "Unix time of last commit")
        self.freshness_p50 = Gauge("lakeflow_stream_freshness_p50_seconds", "p50 (commit - source ts), last batch")
        self.freshness_p95 = Gauge("lakeflow_stream_freshness_p95_seconds", "p95 (commit - source ts), last batch")
        self.freshness_max = Gauge("lakeflow_stream_freshness_max_seconds", "max (commit - source ts), last batch")
        self.query_active = Gauge("lakeflow_stream_query_active", "1 while the streaming query is running")
        self.watermark = Gauge("lakeflow_stream_watermark_timestamp_seconds", "Lateness watermark")
        self.file_bytes = Gauge("lakeflow_stream_avg_added_file_bytes", "Average size of files added", ["table"])
        self.files_written = Counter("lakeflow_stream_data_files_added_total", "Data files added", ["table"])
        self.contract_reloads = Counter("lakeflow_stream_contract_reloads_total", "Contract hot reloads", ["result"])
        self.input_rows_per_second = Gauge("lakeflow_stream_input_rows_per_second", "Spark progress input rate")
        self.processed_rows_per_second = Gauge("lakeflow_stream_processed_rows_per_second", "Spark progress rate")

    def start(self, port: int) -> None:
        if self.enabled:
            start_http_server(port)
