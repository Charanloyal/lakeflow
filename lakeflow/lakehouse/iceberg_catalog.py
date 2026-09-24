"""Apache Iceberg Catalog & MinIO S3 Lakehouse Storage Interface for LakeFlow."""

from __future__ import annotations
from typing import Any, Dict, List, Optional


class IcebergCatalogManager:
    """Manages Apache Iceberg table schemas, metadata trees, and MinIO S3 object store paths."""

    def __init__(
        self,
        warehouse_path: str = "s3a://warehouse/",
        catalog_name: str = "lakeflow",
        endpoint: str = "http://minio:9000",
    ):
        self.warehouse_path = warehouse_path
        self.catalog_name = catalog_name
        self.endpoint = endpoint
        self._tables: Dict[str, Dict[str, Any]] = {
            "silver_customers": {
                "schema": ["customer_id", "email", "full_name", "status", "tier", "created_at"],
                "partition_by": ["tier"],
                "format": "PARQUET",
                "compression": "ZSTD",
                "target_file_size_bytes": 134217728,  # 128 MB target
            },
            "silver_orders": {
                "schema": ["order_id", "customer_id", "total_amount", "order_date", "status"],
                "partition_by": ["order_date"],
                "format": "PARQUET",
                "compression": "ZSTD",
                "target_file_size_bytes": 134217728,
            },
        }

    def get_table_metadata(self, table_name: str) -> Optional[Dict[str, Any]]:
        return self._tables.get(table_name)

    def list_tables(self) -> List[str]:
        return list(self._tables.keys())
