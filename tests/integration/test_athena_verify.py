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


@pytest.mark.integration
@pytest.mark.parametrize("namespace,table_name", CATALOG_CONFIGS)
def test_athena_row_count(
    namespace: str,
    table_name: str,
    migrated_tables,
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
    migrated_tables,
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
    migrated_tables,
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
