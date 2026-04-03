"""Seed an Iceberg table in MinIO via Lakekeeper REST catalog.

Requires: docker compose --profile rest up -d

Usage: uv run python infra/seed/seed_rest.py
"""

from __future__ import annotations

from pyiceberg.catalog.rest import RestCatalog

from infra.seed.common import (
    CATALOG_NAMESPACES,
    TABLE_NAME,
    TABLE_SCHEMA,
    WAREHOUSE,
    s3_properties,
    sample_data,
)

LAKEKEEPER_URI = "http://localhost:8181/catalog"


def main() -> None:
    namespace = CATALOG_NAMESPACES["rest"]

    catalog = RestCatalog(
        "lakekeeper",
        uri=LAKEKEEPER_URI,
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

    # Drop if exists (clean slate for this namespace)
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
