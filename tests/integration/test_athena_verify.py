"""AWS integration test: Athena verification of migrated Iceberg tables.

Requires:
  - Docker compose with all profiles up and seeded (just seed-all)
  - AWS credentials configured
  - Terraform infra applied (just tf-apply)
  - .env with AWS_TEST_BUCKET, GLUE_DATABASE, ATHENA_WORKGROUP

Migration is handled by the session-scoped `migrated_tables` fixture in conftest.py.
This file only exercises Athena queries.

Queries verified per namespace:
  1. Row count (SELECT COUNT(*))              → expect 10
  2. Dimension JOIN (sample_table ⋈ cities)   → expect 5 rows with region populated
  3. Cross-catalog JOIN (rest_ns ⋈ sql_ns)    → expect 3 rows with matching names
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import pytest

from tests.integration.conftest import (
    GLUE_DB,
    run_athena_query,
)

if TYPE_CHECKING:
    from mypy_boto3_athena import AthenaClient

CATALOG_CONFIGS = [
    pytest.param("rest_ns", "sample_table", id="rest"),
    pytest.param("sql_ns", "sample_table", id="sql"),
    pytest.param("hms_ns", "sample_table", id="hms"),
]


def _ms_to_athena_timestamp(ts_ms: int) -> str:
    """Convert epoch milliseconds to Athena TIMESTAMP string.

    Adds 1ms to ensure the query resolves to this snapshot
    (Iceberg returns the snapshot with timestamp <= query timestamp).
    """
    dt = datetime.datetime.fromtimestamp((ts_ms + 1) / 1000.0, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


@pytest.mark.integration
@pytest.mark.parametrize("namespace,table_name", CATALOG_CONFIGS)
def test_athena_row_count(
    namespace: str,
    table_name: str,
    migrated_tables: list[tuple[str, str, str]],
    athena_client: "AthenaClient",
) -> None:
    """Athena COUNT(*) on migrated sample_table returns 10 seeded rows."""
    glue_table = f"{namespace}_{table_name}"
    rows = run_athena_query(
        athena_client,
        f"SELECT COUNT(*) AS cnt FROM {GLUE_DB}.{glue_table}",
    )
    assert len(rows) == 1
    assert int(rows[0][0]) == 10, f"Expected 10 rows, got {rows[0][0]}"


@pytest.mark.integration
@pytest.mark.parametrize("namespace,table_name", CATALOG_CONFIGS)
def test_athena_dimension_join(
    namespace: str,
    table_name: str,
    migrated_tables: list[tuple[str, str, str]],
    athena_client: "AthenaClient",
) -> None:
    """Athena JOIN of sample_table with cities returns 5 rows with region populated."""
    glue_table = f"{namespace}_{table_name}"
    cities_table = f"{namespace}_cities"
    rows = run_athena_query(
        athena_client,
        f"""
        SELECT s.name, c.region
        FROM {GLUE_DB}.{glue_table} s
        JOIN {GLUE_DB}.{cities_table} c ON s.city = c.city_name
        ORDER BY s.id
        LIMIT 5
        """,
    )
    assert len(rows) == 5, f"Expected 5 rows from dimension join, got {len(rows)}"
    regions = {row[1] for row in rows}
    assert regions <= {"Northern Vietnam", "Southern Vietnam", "Central Vietnam"}, (
        f"Unexpected region values: {regions}"
    )


@pytest.mark.integration
def test_athena_cross_catalog_join(
    migrated_tables: list[tuple[str, str, str]],
    athena_client: "AthenaClient",
) -> None:
    """Athena JOIN of rest_ns and sql_ns tables returns 3 rows with matching names."""
    rows = run_athena_query(
        athena_client,
        f"""
        SELECT r.id, r.name AS rest_name, s.name AS sql_name
        FROM {GLUE_DB}.rest_ns_sample_table r
        JOIN {GLUE_DB}.sql_ns_sample_table s ON r.id = s.id
        WHERE r.id <= 3
        ORDER BY r.id
        """,
    )
    assert len(rows) == 3, f"Expected 3 rows from cross-catalog join, got {len(rows)}"
    for row in rows:
        assert row[1] == row[2], (
            f"Names should match across catalogs: rest={row[1]}, sql={row[2]}"
        )


@pytest.mark.integration
@pytest.mark.parametrize("namespace,table_name", CATALOG_CONFIGS)
def test_athena_time_travel_snapshot_1(
    namespace: str,
    table_name: str,
    migrated_tables: list[tuple[str, str, str, list[int]]],
    snapshot_timestamps: dict[str, list[int]],
    athena_client: "AthenaClient",
) -> None:
    """Time-travel to snapshot 1 (initial append): expect 5 rows, SUM(amount) = 801.5."""
    ts_list = snapshot_timestamps[namespace]
    assert len(ts_list) >= 1, f"Expected at least 1 snapshot timestamp for {namespace}"
    s1_ts = _ms_to_athena_timestamp(ts_list[0])

    glue_table = f"{namespace}_{table_name}"

    # Row count
    rows = run_athena_query(
        athena_client,
        f"SELECT COUNT(*) AS cnt FROM {GLUE_DB}.{glue_table} FOR TIMESTAMP AS OF TIMESTAMP '{s1_ts}'",
    )
    assert len(rows) == 1
    assert int(rows[0][0]) == 5, f"S1: Expected 5 rows, got {rows[0][0]}"

    # Sum of amounts
    rows = run_athena_query(
        athena_client,
        f"SELECT CAST(SUM(amount) AS DOUBLE) AS total FROM {GLUE_DB}.{glue_table} FOR TIMESTAMP AS OF TIMESTAMP '{s1_ts}'",
    )
    assert len(rows) == 1
    assert abs(float(rows[0][0]) - 801.5) < 0.01, (
        f"S1: Expected SUM(amount) ~801.5, got {rows[0][0]}"
    )


@pytest.mark.integration
@pytest.mark.parametrize("namespace,table_name", CATALOG_CONFIGS)
def test_athena_time_travel_snapshot_2(
    namespace: str,
    table_name: str,
    migrated_tables: list[tuple[str, str, str, list[int]]],
    snapshot_timestamps: dict[str, list[int]],
    athena_client: "AthenaClient",
) -> None:
    """Time-travel to snapshot 2 (after overwrite): expect 5 rows, SUM(amount) = 2387.25."""
    ts_list = snapshot_timestamps[namespace]
    assert len(ts_list) >= 2, f"Expected at least 2 snapshot timestamps for {namespace}"
    s2_ts = _ms_to_athena_timestamp(ts_list[1])

    glue_table = f"{namespace}_{table_name}"

    # Row count
    rows = run_athena_query(
        athena_client,
        f"SELECT COUNT(*) AS cnt FROM {GLUE_DB}.{glue_table} FOR TIMESTAMP AS OF TIMESTAMP '{s2_ts}'",
    )
    assert len(rows) == 1
    assert int(rows[0][0]) == 5, f"S2: Expected 5 rows, got {rows[0][0]}"

    # Sum of amounts
    rows = run_athena_query(
        athena_client,
        f"SELECT CAST(SUM(amount) AS DOUBLE) AS total FROM {GLUE_DB}.{glue_table} FOR TIMESTAMP AS OF TIMESTAMP '{s2_ts}'",
    )
    assert len(rows) == 1
    assert abs(float(rows[0][0]) - 2387.25) < 0.01, (
        f"S2: Expected SUM(amount) ~2387.25, got {rows[0][0]}"
    )
