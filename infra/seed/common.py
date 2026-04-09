"""Shared table schema, sample data, and MinIO config for seed scripts.

All seed scripts use the same table definition so tests can verify
the migration tool produces identical results regardless of source catalog.
"""

from __future__ import annotations

import datetime
import os
from typing import Any

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
CITIES_TABLE_NAME = "cities"

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


CITIES_SCHEMA = pa.schema(
    [
        pa.field("city_name", pa.string()),
        pa.field("region", pa.string()),
        pa.field("country", pa.string()),
    ]
)


def cities_data() -> pa.Table:
    """Return a PyArrow table with 3 city rows for the dimension join test."""
    return pa.table(
        {
            "city_name": ["Hanoi", "HCMC", "Danang"],
            "region": ["Northern Vietnam", "Southern Vietnam", "Central Vietnam"],
            "country": ["Vietnam", "Vietnam", "Vietnam"],
        },
        schema=CITIES_SCHEMA,
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


# ---------------------------------------------------------------------------
# Multi-snapshot batch data — used by seed_table_with_history()
# ---------------------------------------------------------------------------

# Expected state after each snapshot:
#   S1 (append batch_1):    5 rows, SUM(amount) = 801.5
#   S2 (overwrite id<=2):   5 rows, SUM(amount) = 2387.25
#   S3 (append batch_3):   10 rows, SUM(amount) = 3478.0


def batch_1() -> pa.Table:
    """Rows 1-5: initial append (snapshot 1)."""

    def ts(s: str) -> datetime.datetime:
        return datetime.datetime.fromisoformat(s)

    return pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "city": ["Hanoi", "HCMC", "Danang", "Hanoi", "HCMC"],
            "amount": [100.5, 200.75, 50.0, 300.25, 150.0],
            "created_at": pa.array(
                [
                    ts("2025-01-01T10:00:00"),
                    ts("2025-01-02T11:30:00"),
                    ts("2025-01-03T09:15:00"),
                    ts("2025-01-04T14:00:00"),
                    ts("2025-01-05T16:45:00"),
                ],
                type=pa.timestamp("us"),
            ),
        },
        schema=TABLE_SCHEMA,
    )


def batch_2_overwrite() -> pa.Table:
    """Rows 1-2 with updated amounts: Alice 999.0, Bob 888.0 (snapshot 2).

    Used with overwrite_filter="id <= 2" to replace the original rows.
    """

    def ts(s: str) -> datetime.datetime:
        return datetime.datetime.fromisoformat(s)

    return pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": ["Alice", "Bob"],
            "city": ["Hanoi", "HCMC"],
            "amount": [999.0, 888.0],
            "created_at": pa.array(
                [
                    ts("2025-01-01T10:00:00"),
                    ts("2025-01-02T11:30:00"),
                ],
                type=pa.timestamp("us"),
            ),
        },
        schema=TABLE_SCHEMA,
    )


def batch_3() -> pa.Table:
    """Rows 6-10: second append (snapshot 3)."""

    def ts(s: str) -> datetime.datetime:
        return datetime.datetime.fromisoformat(s)

    return pa.table(
        {
            "id": pa.array([6, 7, 8, 9, 10], type=pa.int64()),
            "name": ["Frank", "Grace", "Huy", "Ivy", "Jack"],
            "city": ["Danang", "Hanoi", "HCMC", "Danang", "Hanoi"],
            "amount": [75.0, 425.5, 180.0, 90.25, 320.0],
            "created_at": pa.array(
                [
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


def seed_table_with_history(table: Any) -> None:
    """Seed a table with 3 snapshots: append, overwrite, append.

    Creates a deterministic history for time-travel verification:
      S1: append rows 1-5
      S2: overwrite rows where id <= 2 with new amounts
      S3: append rows 6-10

    Args:
        table: A PyIceberg Table object (must support append/overwrite).
    """
    table.append(batch_1())
    table.overwrite(batch_2_overwrite(), overwrite_filter="id <= 2")
    table.append(batch_3())
