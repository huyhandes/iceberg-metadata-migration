"""AWS integration test: invoke the deployed test Lambda end-to-end.

Requires:
  - Docker compose with all profiles up and seeded (just seed-all)
  - AWS credentials configured
  - Terraform infra applied, including the test Lambda (just tf-apply)
  - .env with AWS_TEST_BUCKET, AWS_REGION, GLUE_DATABASE, ATHENA_WORKGROUP,
    LAMBDA_FUNCTION_NAME (defaults to "iceberg-migration-lambda")

Proves the Lambda path (container image, handler, IAM, VPC) by invoking the
deployed function to perform its OWN migration of a fresh Glue table, then
reuses the Athena verify path to confirm the result is queryable.

Example invocation payload (event keys mirror CLI args 1:1, snake_case):

    {
        "table_location": "s3://<bucket>/warehouse/rest_ns/sample_table",
        "source_prefix": "s3://warehouse/rest_ns",
        "dest_prefix": "s3://<bucket>/warehouse/rest_ns",
        "glue_database": "iceberg_migration_test",
        "glue_table": "rest_ns_sample_table_lambda",
        "aws_region": "us-east-1"
    }

Required keys: table_location, source_prefix, dest_prefix.
Optional keys: glue_database, glue_table, dry_run, verbose, aws_region.
On success the handler returns {"status": "success", ...}; on failure it
raises, surfacing as a Lambda FunctionError.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

import boto3
import pytest

from tests.integration.conftest import (
    AWS_BUCKET,
    AWS_REGION,
    GLUE_DB,
    cleanup_glue_table,
    run_athena_query,
)

if TYPE_CHECKING:
    from mypy_boto3_athena import AthenaClient
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_lambda import LambdaClient

# Terraform output; mirrors how conftest reads EMR_APPLICATION_ID etc.
LAMBDA_FUNCTION_NAME = os.environ.get(
    "LAMBDA_FUNCTION_NAME", "iceberg-migration-lambda"
)


@pytest.fixture(scope="session")
def lambda_client() -> "LambdaClient":
    """Boto3 Lambda client for real AWS."""
    return boto3.client("lambda", region_name=AWS_REGION)


@pytest.mark.integration
def test_lambda_invoke_creates_queryable_table(
    migrated_tables: list[tuple[str, str, str, dict[str, int]]],
    lambda_client: "LambdaClient",
    athena_client: "AthenaClient",
    glue_client: "GlueClient",
) -> None:
    """Invoke the deployed Lambda to migrate a fresh Glue table, then verify via Athena.

    Targets the rest_ns/sample_table location already synced to S3 by the
    session fixture, but registers under a `_lambda`-suffixed Glue table name
    so the Lambda's own CreateTable runs. The migration is idempotent and
    non-destructive (writes under `_migrated/`, originals untouched), so
    re-running over the shared location is safe.
    """
    namespace = "rest_ns"
    table_name = "sample_table"
    glue_table = f"{namespace}_{table_name}_lambda"

    event = {
        "table_location": f"s3://{AWS_BUCKET}/warehouse/{namespace}/{table_name}",
        "source_prefix": f"s3://warehouse/{namespace}",
        "dest_prefix": f"s3://{AWS_BUCKET}/warehouse/{namespace}",
        "glue_database": GLUE_DB,
        "glue_table": glue_table,
        "aws_region": AWS_REGION,
    }

    try:
        response = lambda_client.invoke(
            FunctionName=LAMBDA_FUNCTION_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps(event),
        )

        # A failed Migration raises in the handler -> Lambda returns FunctionError.
        assert not response.get("FunctionError"), (
            f"Lambda {LAMBDA_FUNCTION_NAME} returned FunctionError "
            f"{response['FunctionError']}: "
            f"{response['Payload'].read().decode()}"
        )

        payload = json.loads(response["Payload"].read())
        assert payload.get("status") == "success", (
            f"Unexpected Lambda response status: {payload}"
        )

        # Reuse the Athena verify path: the Lambda-migrated table is queryable.
        rows = run_athena_query(
            athena_client,
            f"SELECT COUNT(*) AS cnt FROM {GLUE_DB}.{glue_table}",
        )
        assert len(rows) == 1
        assert int(rows[0][0]) == 10, f"Expected 10 rows, got {rows[0][0]}"
    finally:
        cleanup_glue_table(glue_client, GLUE_DB, glue_table)
