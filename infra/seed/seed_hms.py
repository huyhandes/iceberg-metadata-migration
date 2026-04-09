"""Seed an Iceberg table in MinIO for the HMS (Hive Metastore) test namespace.

The hms_ns tests only verify that metadata exists in MinIO and that the
migration CLI can rewrite paths — they don't query through Hive. We seed
using a SQLite catalog so we avoid the Hive S3A classpath dependency
(hadoop-aws + aws-java-sdk-bundle jars not bundled in the image).

The table location uses s3://warehouse/hms_ns/... matching the paths the
integration test expects to rewrite.

Requires: docker compose up -d minio minio-init
"""

from __future__ import annotations

import os

from pyiceberg.catalog.sql import SqlCatalog

from infra.seed.common import (
    CATALOG_NAMESPACES,
    CITIES_SCHEMA,
    CITIES_TABLE_NAME,
    TABLE_NAME,
    TABLE_SCHEMA,
    WAREHOUSE,
    cities_data,
    s3_properties,
    seed_table_with_history,
)

# Separate SQLite DB from the sql_ns seed to avoid namespace collisions
SQLITE_DB = os.environ.get("HMS_SQLITE_DB", "sqlite:///infra/seed/hms_catalog.db")


def main() -> None:
    namespace = CATALOG_NAMESPACES["hms"]

    catalog = SqlCatalog(
        "hms_local",
        uri=SQLITE_DB,
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
    seed_table_with_history(table)

    # Verify
    table = catalog.load_table(table_id)
    metadata = table.metadata
    print(f"Created {namespace}.{TABLE_NAME}")
    print(f"  Location: {metadata.location}")
    print(f"  Snapshots: {len(metadata.snapshots)}")
    print(f"  Metadata: {table.metadata_location}")

    # Seed cities dimension table
    cities_id = (namespace, CITIES_TABLE_NAME)
    try:
        catalog.drop_table(cities_id)
    except Exception:
        pass

    cities_table = catalog.create_table(
        identifier=cities_id,
        schema=CITIES_SCHEMA,
        location=f"{WAREHOUSE}/{namespace}/{CITIES_TABLE_NAME}",
    )
    cities_table.append(cities_data())
    print(f"Created {namespace}.{CITIES_TABLE_NAME}")


if __name__ == "__main__":
    main()
