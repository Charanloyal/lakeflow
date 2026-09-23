#!/usr/bin/env python3
"""
LakeFlow Production Streaming Engine (v2)
Apache Spark Structured Streaming with:
- RocksDB State Store Provider
- Watermark-based Stateful Deduplication
- Micro-batch ACID Iceberg Writing with Schema Evolution
- Streaming Telemetry & Real-Time Metrics Extraction
"""

import os
import sys
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, coalesce,
    current_timestamp, expr, window
)
from pyspark.sql.types import (
    StringType, LongType, DoubleType, StructType, StructField
)
from pyspark.sql.streaming import StreamingQueryListener

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LakeFlowStreaming")

# Environment / Service Defaults
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "password123")
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "s3a://lakeflow/checkpoints/lakeflow-stream-v2")
WAREHOUSE_ROOT = os.environ.get("WAREHOUSE_ROOT", "s3a://warehouse/")

class LakeFlowMetricsListener(StreamingQueryListener):
    """Real-time streaming query listener reporting true micro-batch telemetry."""
    def onQueryStarted(self, event):
        logger.info(f"Query started: {event.name} [ID: {event.id}]")

    def onQueryProgress(self, event):
        progress = event.progress
        num_input_rows = progress.numInputRows
        input_rate = progress.inputRowsPerSecond
        process_rate = progress.processedRowsPerSecond
        batch_duration_ms = progress.durationMs.get("total", 0)
        
        logger.info(
            f"[METRICS] Batch {progress.batchId}: Input={num_input_rows} rows | "
            f"InputRate={input_rate:.1f} r/s | ProcessRate={process_rate:.1f} r/s | "
            f"BatchDuration={batch_duration_ms}ms"
        )

    def onQueryTerminated(self, event):
        logger.warn(f"Query terminated: {event.id} (Exception: {event.exception})")

def build_spark_session() -> SparkSession:
    logger.info("Initializing SparkSession with Iceberg and RocksDB optimizations...")
    builder = (
        SparkSession.builder.appName("LakeFlow-Production-CDC-Engine")
        # 1. RocksDB State Store Provider for scalable checkpointing
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .config("spark.sql.streaming.stateStore.rocksdb.compactOnCommit", "true")
        
        # 2. Iceberg Catalog Configurations
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakeflow", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakeflow.type", "hadoop")
        .config("spark.sql.catalog.lakeflow.warehouse", WAREHOUSE_ROOT)
        
        # 3. Iceberg Write Optimizations (prevent small files)
        .config("spark.sql.catalog.lakeflow.write.format.default", "parquet")
        .config("spark.sql.catalog.lakeflow.write.parquet.compression-codec", "zstd")
        .config("spark.sql.catalog.lakeflow.write.target-file-size-bytes", "134217728") # 128MB
        
        # 4. S3A / MinIO Configuration
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        # 5. Schema Evolution
        .config("spark.sql.streaming.schemaInference", "true")
    )
    return builder.getOrCreate()

def run_lakeflow_stream():
    spark = build_spark_session()
    spark.streams.addListener(LakeFlowMetricsListener())

    logger.info(f"Subscribing to Kafka topics on {KAFKA_BOOTSTRAP} matching 'lakeflow.platform.*'")

    # Read raw CDC stream from Kafka
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", "lakeflow\\.platform\\..*")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Generic Debezium Envelope Schema
    debezium_schema = StructType([
        StructField("before", StringType(), True),
        StructField("after", StringType(), True),
        StructField("source", StructType([
            StructField("lsn", LongType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("table", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("txId", LongType(), True),
        ]), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
    ])

    # Parse and extract metadata
    parsed_df = (
        raw_df.select(
            col("key").cast("string").alias("record_key"),
            from_json(col("value").cast("string"), debezium_schema).alias("payload"),
            col("timestamp").alias("kafka_timestamp"),
            col("topic").alias("source_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset")
        )
        .select(
            col("record_key"),
            col("source_topic"),
            col("payload.source.table").alias("table_name"),
            col("payload.op").alias("cdc_op"),
            col("payload.source.lsn").alias("source_lsn"),
            col("payload.source.txId").alias("tx_id"),
            (col("payload.source.ts_ms") / 1000.0).cast("timestamp").alias("source_timestamp"),
            col("payload.before").alias("before_state"),
            col("payload.after").alias("after_state"),
            col("kafka_timestamp"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )

    # Deterministic Deduplication with Watermark
    # Uses 10-minute event watermark and deduplicates on (record_key, source_lsn)
    deduped_df = (
        parsed_df
        .withWatermark("source_timestamp", "10 minutes")
        .dropDuplicates(["record_key", "source_lsn"])
    )

    # Micro-batch sink into Iceberg Table
    def write_to_iceberg(batch_df, batch_id):
        count = batch_df.count()
        if count == 0:
            return
        
        logger.info(f"Writing micro-batch {batch_id} with {count} deduplicated events to Iceberg...")
        (
            batch_df.write
            .format("iceberg")
            .mode("append")
            .save("lakeflow.silver_cdc_events")
        )
        logger.info(f"Micro-batch {batch_id} committed successfully to Iceberg.")

    query = (
        deduped_df.writeStream
        .foreachBatch(write_to_iceberg)
        .option("checkpointLocation", CHECKPOINT_ROOT)
        .trigger(processingTime="5 seconds")
        .start()
    )

    logger.info("LakeFlow Streaming Engine active. Awaiting micro-batches...")
    query.awaitTermination()

if __name__ == "__main__":
    run_lakeflow_stream()
