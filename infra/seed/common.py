"""Shared table schema, sample data, and MinIO config for seed scripts.

All seed scripts use the same table definition so tests can verify
the migration tool produces identical results regardless of source catalog.
"""

from __future__ import annotations

import datetime
import os

import pyarrow as pa

# ---------------------------------------------------------------------------
# MinIO connection constants
# ---------------------------------------------------------------------------

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_REGION = "us-east-1"
WAREHOUSE = "s3://warehouse"

# ---------------------------------------------------------------------------
# Per-catalog namespace isolation
# ---------------------------------------------------------------------------

CATALOG_NAMESPACES = {
    "rest": "rest_ns",
    "sql": "sql_ns",
    "hms": "hms_ns",
}

TABLE_NAME = "sample_table"

# ---------------------------------------------------------------------------
# Shared table schema
# ---------------------------------------------------------------------------

TABLE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("city", pa.string()),
        pa.field("amount", pa.float64()),
        pa.field("created_at", pa.timestamp("us")),
    ]
)


def sample_data() -> pa.Table:
    """Return a PyArrow table with 10 sample rows for seeding."""

    def ts(s: str) -> datetime.datetime:
        return datetime.datetime.fromisoformat(s)

    return pa.table(
        {
            "id": pa.array(list(range(1, 11)), type=pa.int64()),
            "name": [
                "Alice",
                "Bob",
                "Charlie",
                "Diana",
                "Eve",
                "Frank",
                "Grace",
                "Huy",
                "Ivy",
                "Jack",
            ],
            "city": [
                "Hanoi",
                "HCMC",
                "Danang",
                "Hanoi",
                "HCMC",
                "Danang",
                "Hanoi",
                "HCMC",
                "Danang",
                "Hanoi",
            ],
            "amount": [
                100.5,
                200.75,
                50.0,
                300.25,
                150.0,
                75.0,
                425.5,
                180.0,
                90.25,
                320.0,
            ],
            "created_at": pa.array(
                [
                    ts("2025-01-01T10:00:00"),
                    ts("2025-01-02T11:30:00"),
                    ts("2025-01-03T09:15:00"),
                    ts("2025-01-04T14:00:00"),
                    ts("2025-01-05T16:45:00"),
                    ts("2025-02-01T08:00:00"),
                    ts("2025-02-02T10:30:00"),
                    ts("2025-02-03T12:15:00"),
                    ts("2025-02-04T15:00:00"),
                    ts("2025-02-05T17:45:00"),
                ],
                type=pa.timestamp("us"),
            ),
        },
        schema=TABLE_SCHEMA,
    )


def s3_properties() -> dict[str, str]:
    """Return PyIceberg S3 FileIO properties for MinIO."""
    return {
        "s3.endpoint": MINIO_ENDPOINT,
        "s3.access-key-id": MINIO_ACCESS_KEY,
        "s3.secret-access-key": MINIO_SECRET_KEY,
        "s3.region": MINIO_REGION,
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    }
