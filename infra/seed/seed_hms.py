"""Seed an Iceberg table in MinIO via Hive Metastore catalog.

Requires: docker compose --profile hms up -d

Usage: uv run python infra/seed/seed_hms.py
"""

from __future__ import annotations

import os

from pyiceberg.catalog.hive import HiveCatalog

from infra.seed.common import (
    CATALOG_NAMESPACES,
    TABLE_NAME,
    TABLE_SCHEMA,
    WAREHOUSE,
    s3_properties,
    sample_data,
)

HMS_URI = os.environ.get("HMS_URI", "thrift://localhost:9083")


def main() -> None:
    namespace = CATALOG_NAMESPACES["hms"]

    catalog = HiveCatalog(
        "hive_local",
        uri=HMS_URI,
        warehouse=WAREHOUSE,
        **s3_properties(),
    )

    # Create namespace if not exists
    try:
        catalog.create_namespace(namespace)
        print(f"Created namespace: {namespace}")
    except Exception:
        print(f"Namespace {namespace} already exists")

    table_id = (namespace, TABLE_NAME)

    # Drop if exists
    try:
        catalog.drop_table(table_id)
        print(f"Dropped existing table: {namespace}.{TABLE_NAME}")
    except Exception:
        pass

    # Create and populate
    table = catalog.create_table(
        identifier=table_id,
        schema=TABLE_SCHEMA,
        location=f"{WAREHOUSE}/{namespace}/{TABLE_NAME}",
    )
    table.append(sample_data())

    # Verify
    table = catalog.load_table(table_id)
    metadata = table.metadata
    print(f"Created {namespace}.{TABLE_NAME}")
    print(f"  Location: {metadata.location}")
    print(f"  Snapshots: {len(metadata.snapshots)}")
    print(f"  Metadata: {table.metadata_location}")


if __name__ == "__main__":
    main()
