from __future__ import annotations

from pyspark.sql import SparkSession

from .config import Settings

COMMON_CONF = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.streaming.metricsEnabled": "true",
    "spark.ui.showConsoleProgress": "false",
}


def build_session(settings: Settings) -> SparkSession:
    c = settings.catalog
    builder = SparkSession.builder.appName(f"lakeflow-{settings.pipeline}")
    conf = {
        **COMMON_CONF,
        f"spark.sql.catalog.{c}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{c}.type": "rest",
        f"spark.sql.catalog.{c}.uri": settings.catalog_uri,
        f"spark.sql.catalog.{c}.warehouse": settings.warehouse,
        f"spark.sql.catalog.{c}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{c}.s3.endpoint": settings.s3_endpoint,
        f"spark.sql.catalog.{c}.s3.path-style-access": "true",
        # External writers (Trino maintenance) commit too: always read the latest table metadata.
        f"spark.sql.catalog.{c}.cache-enabled": "false",
        "spark.sql.defaultCatalog": c,
        "spark.sql.shuffle.partitions": str(settings.shuffle_partitions),
    }
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def build_local_test_session(
    warehouse_dir: str, catalog: str = "lakehouse", packages: str | None = None
) -> SparkSession:
    """Local Hadoop-catalog session for PySpark tests (no Kafka/MinIO needed)."""
    builder = SparkSession.builder.master("local[2]").appName("lakeflow-tests")
    conf = {
        **COMMON_CONF,
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.type": "hadoop",
        f"spark.sql.catalog.{catalog}.warehouse": warehouse_dir,
        f"spark.sql.catalog.{catalog}.cache-enabled": "false",
        "spark.sql.defaultCatalog": catalog,
        "spark.sql.shuffle.partitions": "2",
        "spark.default.parallelism": "2",
        "spark.ui.enabled": "false",
    }
    if packages:
        conf["spark.jars.packages"] = packages
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
