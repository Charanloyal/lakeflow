"""Streaming job entry point (spark-submit /opt/lakeflow/jobs/cdc_stream.py)."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

from lakeflow_core.contracts import load_registry
from pyspark.sql.streaming import StreamingQueryListener

from .config import Settings
from .control import ControlChannel
from .metrics import PipelineMetrics
from .session import build_session
from .sink import SinkContext, ensure_tables, process_batch

log = logging.getLogger("lakeflow.job")


class ProgressListener(StreamingQueryListener):
    def __init__(self, metrics: PipelineMetrics):
        self.metrics = metrics

    def onQueryStarted(self, event):  # noqa: N802 - Spark API
        log.info("query started id=%s run=%s", event.id, event.runId)

    def onQueryProgress(self, event):  # noqa: N802
        progress = event.progress
        self.metrics.input_rows_per_second.set(progress.inputRowsPerSecond or 0.0)
        self.metrics.processed_rows_per_second.set(progress.processedRowsPerSecond or 0.0)

    def onQueryIdle(self, event):  # noqa: N802
        pass

    def onQueryTerminated(self, event):  # noqa: N802
        self.metrics.query_active.set(0)
        log.error("query terminated id=%s exception=%s", event.id, event.exception)


def ensure_epoch(checkpoint_dir: str) -> str:
    """Identifier of this checkpoint lineage; a wiped checkpoint gets a new one, so batch ids never collide."""
    root = Path(checkpoint_dir)
    root.mkdir(parents=True, exist_ok=True)
    epoch_file = root / "stream-epoch"
    if epoch_file.exists() and (root / "query" / "metadata").exists():
        return epoch_file.read_text(encoding="utf-8").strip()
    epoch = uuid.uuid4().hex[:12]
    epoch_file.write_text(epoch, encoding="utf-8")
    return epoch


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    metrics = PipelineMetrics()
    metrics.start(settings.metrics_port)
    registry = load_registry(settings.contracts_dir)
    spark = build_session(settings)
    spark.sparkContext.setLogLevel("WARN")
    epoch = ensure_epoch(settings.checkpoint_dir)
    ensure_tables(spark, registry, settings.catalog)
    control = ControlChannel(settings.control_dir)
    control.start()
    ctx = SinkContext(
        spark=spark,
        settings=settings,
        registry=registry,
        epoch=epoch,
        app_id=spark.sparkContext.applicationId,
        metrics=metrics,
        control=control,
        publish_ops=True,
    )
    spark.streams.addListener(ProgressListener(metrics))
    log.info("starting stream epoch=%s topics=%s trigger=%ss", epoch, settings.topics, settings.trigger_seconds)
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", ",".join(settings.topics))
        .option("startingOffsets", settings.starting_offsets)
        .option("maxOffsetsPerTrigger", str(settings.max_offsets_per_trigger))
        .option("includeHeaders", "true")
        .option("failOnDataLoss", str(settings.fail_on_data_loss).lower())
        .load()
    )
    query = (
        source.writeStream.queryName(settings.pipeline)
        .foreachBatch(lambda df, batch_id: process_batch(ctx, df, batch_id))
        .option("checkpointLocation", f"{settings.checkpoint_dir}/query")
        .trigger(processingTime=f"{settings.trigger_seconds} seconds")
        .start()
    )
    metrics.query_active.set(1)
    query.awaitTermination()
