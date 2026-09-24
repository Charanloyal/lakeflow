"""LakeFlow Streaming Engine: Spark Structured Streaming with RocksDB & Watermarked Deduplication."""

from lakeflow.streaming.deduplicator import CDCDeduplicator
from lakeflow.streaming.checkpoint_manager import CheckpointManager

__all__ = ["CDCDeduplicator", "CheckpointManager"]
