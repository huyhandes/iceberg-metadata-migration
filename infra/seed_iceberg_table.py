"""Seed a test Iceberg table in MinIO using PyIceberg with a SQL catalog.

Creates a table with sample data and multiple snapshots to test
the migration tool against realistic metadata.

Usage:
    uv run python infra/seed_iceberg_table.py

Requires: docker compose up (MinIO running on localhost:9000)
"""
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

MINIO_ENDPOINT = "http://localhost:9000"
WAREHOUSE = "s3://warehouse"


def main():
    # Use SQL catalog (sqlite) writing to MinIO via s3fs
    catalog = SqlCatalog(
        "local",
        **{
            "uri": "sqlite:///infra/iceberg_catalog.db",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.region": "us-east-1",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "warehouse": WAREHOUSE,
        },
    )

    # Create namespace if not exists
    try:
        catalog.create_namespace("test_db")
        print("Created namespace: test_db")
    except Exception:
        print("Namespace test_db already exists")

    # Define schema
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("city", pa.string()),
        pa.field("amount", pa.float64()),
        pa.field("created_at", pa.timestamp("us")),
    ])

    table_id = ("test_db", "orders")

    # Drop if exists (clean slate)
    try:
        catalog.drop_table(table_id)
        print("Dropped existing table: test_db.orders")
    except Exception:
        pass

    # Create table
    table = catalog.create_table(
        identifier=table_id,
        schema=schema,
        location=f"{WAREHOUSE}/test_db/orders",
    )
    print(f"Created table: test_db.orders")
    print(f"  Location: {table.location()}")

    # Arrow schema matching the Iceberg table (id is non-nullable, created_at is timestamp)
    arrow_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("city", pa.string()),
        pa.field("amount", pa.float64()),
        pa.field("created_at", pa.timestamp("us")),
    ])

    import datetime

    def ts(s: str) -> datetime.datetime:
        return datetime.datetime.fromisoformat(s)

    # Snapshot 1: Initial batch
    batch1 = pa.table({
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "city": ["Hanoi", "HCMC", "Danang", "Hanoi", "HCMC"],
        "amount": [100.50, 200.75, 50.00, 300.25, 150.00],
        "created_at": pa.array([ts("2025-01-01T10:00:00"), ts("2025-01-02T11:30:00"), ts("2025-01-03T09:15:00"), ts("2025-01-04T14:00:00"), ts("2025-01-05T16:45:00")], type=pa.timestamp("us")),
    }, schema=arrow_schema)
    table.append(batch1)
    print(f"  Snapshot 1: 5 rows (initial batch)")

    # Snapshot 2: More data
    batch2 = pa.table({
        "id": pa.array([6, 7, 8, 9, 10], type=pa.int64()),
        "name": ["Frank", "Grace", "Huy", "Ivy", "Jack"],
        "city": ["Danang", "Hanoi", "HCMC", "Danang", "Hanoi"],
        "amount": [75.00, 425.50, 180.00, 90.25, 320.00],
        "created_at": pa.array([ts("2025-02-01T08:00:00"), ts("2025-02-02T10:30:00"), ts("2025-02-03T12:15:00"), ts("2025-02-04T15:00:00"), ts("2025-02-05T17:45:00")], type=pa.timestamp("us")),
    }, schema=arrow_schema)
    table.append(batch2)
    print(f"  Snapshot 2: 5 more rows (second batch)")

    # Snapshot 3: Another batch
    batch3 = pa.table({
        "id": pa.array([11, 12, 13], type=pa.int64()),
        "name": ["Kim", "Leo", "Mai"],
        "city": ["HCMC", "Hanoi", "Danang"],
        "amount": [500.00, 60.75, 210.50],
        "created_at": pa.array([ts("2025-03-01T09:00:00"), ts("2025-03-02T11:30:00"), ts("2025-03-03T14:00:00")], type=pa.timestamp("us")),
    }, schema=arrow_schema)
    table.append(batch3)
    print(f"  Snapshot 3: 3 more rows (third batch)")

    # Print metadata info
    table = catalog.load_table(table_id)
    metadata = table.metadata
    print(f"\n--- Table Metadata ---")
    print(f"  Format version: {metadata.format_version}")
    print(f"  Snapshots: {len(metadata.snapshots)}")
    print(f"  Current snapshot ID: {metadata.current_snapshot_id}")
    print(f"  Location: {metadata.location}")
    for snap in metadata.snapshots:
        print(f"  Snapshot {snap.snapshot_id}: manifest-list = {snap.manifest_list}")

    print(f"\nDone! Table test_db.orders ready in MinIO with {len(metadata.snapshots)} snapshots.")
    print(f"Metadata location: {table.metadata_location}")


if __name__ == "__main__":
    main()
