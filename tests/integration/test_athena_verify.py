"""AWS integration test: full round-trip migration with Athena verification.

Requires:
  - Docker compose with all profiles up and seeded
  - AWS_PROFILE configured in .env
  - Terraform infra applied (Glue DB, Athena workgroup)

Flow per catalog type:
  1. Sync data from MinIO to AWS S3
  2. Run migration CLI (rewrite paths, register in Glue)
  3. Athena SELECT to verify queryability
  4. Cleanup (S3 objects + Glue table)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from typer.testing import CliRunner

from iceberg_migrate.cli import app
from tests.integration.conftest import (
    AWS_BUCKET,
    AWS_REGION,
    GLUE_DB,
    MINIO_BUCKET,
    cleanup_glue_table,
    cleanup_s3_prefix,
    run_athena_query,
    sync_minio_to_s3,
)

if TYPE_CHECKING:
    from mypy_boto3_athena import AthenaClient
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_s3 import S3Client

# ---------------------------------------------------------------------------
# Per-catalog test parameters
# ---------------------------------------------------------------------------

CATALOG_CONFIGS = [
    pytest.param(
        "rest_ns",
        "sample_table",
        id="rest",
    ),
    pytest.param(
        "sql_ns",
        "sample_table",
        id="sql",
    ),
    pytest.param(
        "hms_ns",
        "sample_table",
        id="hms",
    ),
]

runner = CliRunner()


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.parametrize("namespace,table_name", CATALOG_CONFIGS)
def test_migrated_table_queryable_via_athena(
    namespace: str,
    table_name: str,
    minio_client: S3Client,
    aws_s3_client: S3Client,
    glue_client: GlueClient,
    athena_client: AthenaClient,
):
    """Full round-trip: seed on MinIO -> sync to S3 -> migrate -> Athena SELECT."""

    glue_table = f"{namespace}_{table_name}"
    s3_prefix = f"warehouse/{namespace}/"

    try:
        # Step 1: Sync from MinIO to AWS S3
        count = sync_minio_to_s3(minio_client, aws_s3_client, namespace)
        assert count > 0, (
            f"No objects synced for {namespace}. "
            f"Did you seed? Run: uv run python infra/seed/seed_{namespace.replace('_ns', '')}.py"
        )

        # Step 2: Run migration CLI
        table_location = f"s3://{AWS_BUCKET}/warehouse/{namespace}/{table_name}"
        src_prefix = f"s3://warehouse/{namespace}"
        dst_prefix = f"s3://{AWS_BUCKET}/warehouse/{namespace}"

        result = runner.invoke(
            app,
            [
                "--table-location", table_location,
                "--source-prefix", src_prefix,
                "--dest-prefix", dst_prefix,
                "--glue-database", GLUE_DB,
                "--glue-table", glue_table,
                "--aws-region", AWS_REGION,
            ],
        )

        assert result.exit_code == 0, (
            f"Migration CLI failed for {namespace} with exit {result.exit_code}.\n"
            f"Output:\n{result.output}"
        )

        # Step 3: Verify via Athena
        # 3a: Row count
        rows = run_athena_query(
            athena_client,
            f"SELECT COUNT(*) as cnt FROM {GLUE_DB}.{glue_table}",
        )
        assert len(rows) == 1, f"Expected 1 result row, got {len(rows)}"
        row_count = int(rows[0][0])
        assert row_count == 10, (
            f"Expected 10 rows (seeded), got {row_count}"
        )

        # 3b: Data integrity — spot check specific values
        data_rows = run_athena_query(
            athena_client,
            f"SELECT id, name FROM {GLUE_DB}.{glue_table} ORDER BY id LIMIT 3",
        )
        assert len(data_rows) == 3, f"Expected 3 rows, got {len(data_rows)}"
        assert data_rows[0][1] == "Alice", f"Expected Alice, got {data_rows[0][1]}"
        assert data_rows[1][1] == "Bob", f"Expected Bob, got {data_rows[1][1]}"
        assert data_rows[2][1] == "Charlie", f"Expected Charlie, got {data_rows[2][1]}"

    finally:
        # Step 4: Cleanup — always runs, even on test failure
        cleanup_s3_prefix(aws_s3_client, AWS_BUCKET, s3_prefix)
        cleanup_glue_table(glue_client, GLUE_DB, glue_table)
