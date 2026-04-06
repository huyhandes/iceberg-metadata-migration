"""AWS integration test: EMR Serverless verification of migrated Iceberg tables.

Requires:
  - Docker compose with all profiles up and seeded (just seed-all)
  - AWS credentials configured
  - Terraform infra applied (just tf-apply)
  - .env with AWS_TEST_BUCKET, EMR_APPLICATION_ID, EMR_JOB_ROLE_ARN

Submits verify_emr.py as an EMR Serverless Spark job run (EMR 7.x).
Polls until SUCCESS, reads results.json from S3, asserts query outcomes.

Migration is handled by the session-scoped `migrated_tables` fixture in conftest.py.

Queries verified per namespace:
  1. Row count                   -> expect 10
  2. Dimension JOIN (⋈ cities)   -> expect 5 rows
  3. Cross-catalog JOIN          -> expect 3 matching rows
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

from tests.integration.conftest import (
    AWS_BUCKET,
    AWS_REGION,
    EMR_APPLICATION_ID,
    EMR_JOB_ROLE_ARN,
    GLUE_DB,
    cleanup_s3_prefix,
    read_job_results,
    run_emr_job,
)

if TYPE_CHECKING:
    from mypy_boto3_emr_serverless import EMRServerlessClient
    from mypy_boto3_s3 import S3Client

CATALOG_CONFIGS = [
    pytest.param("rest_ns", id="rest"),
    pytest.param("sql_ns", id="sql"),
    pytest.param("hms_ns", id="hms"),
]

EMR_SCRIPT_S3_URI = f"s3://{AWS_BUCKET}/spark-jobs/verify_emr.py"


@pytest.mark.integration
@pytest.mark.parametrize("namespace", CATALOG_CONFIGS)
def test_emr_verifies_migrated_table(
    namespace: str,
    migrated_tables: list[tuple[str, str, str]],
    emrserverless_client: "EMRServerlessClient",
    aws_s3_client: "S3Client",
) -> None:
    """EMR Serverless job queries migrated Iceberg table and asserts all 3 query outcomes."""
    run_id = uuid.uuid4().hex[:8]
    output_path = f"s3://{AWS_BUCKET}/integration-results/emr/{namespace}/{run_id}"

    try:
        run_emr_job(
            emrserverless_client,
            EMR_APPLICATION_ID,
            EMR_JOB_ROLE_ARN,
            EMR_SCRIPT_S3_URI,
            entry_point_args=[
                "--output_path",
                output_path,
                "--glue_database",
                GLUE_DB,
                "--namespace",
                namespace,
                "--cross_ns1",
                "rest_ns",
                "--cross_ns2",
                "sql_ns",
                "--aws_region",
                AWS_REGION,
            ],
        )

        results = read_job_results(aws_s3_client, output_path)

        assert results["row_count"] == 10, (
            f"[{namespace}] Expected 10 rows, got {results['row_count']}"
        )
        assert len(results["join_rows"]) == 5, (
            f"[{namespace}] Expected 5 dimension join rows, got {len(results['join_rows'])}"
        )
        regions = {r["region"] for r in results["join_rows"]}
        assert regions <= {"Northern Vietnam", "Southern Vietnam", "Central Vietnam"}, (
            f"[{namespace}] Unexpected region values: {regions}"
        )
        assert len(results["cross_join_rows"]) == 3, (
            f"[{namespace}] Expected 3 cross-catalog join rows, got {len(results['cross_join_rows'])}"
        )
        for row in results["cross_join_rows"]:
            assert row["rest_name"] == row["sql_name"], (
                f"[{namespace}] Names should match: rest={row['rest_name']}, sql={row['sql_name']}"
            )

    finally:
        cleanup_s3_prefix(
            aws_s3_client,
            AWS_BUCKET,
            f"integration-results/emr/{namespace}/{run_id}/",
        )
