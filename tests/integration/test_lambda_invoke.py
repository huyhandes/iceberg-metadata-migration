"""Differential-equivalence gate: CLI and Lambda produce identical S3 artifacts and Glue state."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

import boto3
import pytest
from typer.testing import CliRunner

from iceberg_migrate.cli import app as migrate_app
from tests.fixtures.v3_manifest_with_dv import create_test_metadata
from tests.integration.conftest import (
    AWS_BUCKET,
    AWS_REGION,
    GLUE_DB,
    cleanup_glue_table,
    cleanup_s3_prefix,
)

if TYPE_CHECKING:
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_lambda import LambdaClient
    from mypy_boto3_s3 import S3Client

SANDBOX_LAMBDA_FUNCTION_NAME = os.environ.get(
    "SANDBOX_LAMBDA_FUNCTION_NAME", "iceberg-migration-sandbox"
)

TABLE_PREFIX = "warehouse/lambda-release-gate/testtable"
SRC_PREFIX = "s3a://old-warehouse/data"
GLUE_TABLE = "lambda_release_gate_testtable"


def _snapshot_s3(s3_client, bucket, prefix):
    """Return {key: bytes} for all objects under prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    result = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = s3_client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            result[obj["Key"]] = body
    return result


def _snapshot_glue(glue_client, database, table):
    """Return the user-defined Glue table Parameters (table_type, metadata_location)."""
    return glue_client.get_table(DatabaseName=database, Name=table)["Table"][
        "Parameters"
    ]


@pytest.fixture(scope="session")
def lambda_client() -> "LambdaClient":
    return boto3.client("lambda", region_name=AWS_REGION)


@pytest.mark.integration
def test_lambda_differential_equivalence(
    aws_s3_client: "S3Client",
    glue_client: "GlueClient",
    lambda_client: "LambdaClient",
) -> None:
    """CLI and Lambda produce bit-identical _migrated/ artifacts and Glue parameters."""
    migrated_prefix = f"{TABLE_PREFIX}/_migrated/"

    create_test_metadata(
        aws_s3_client, AWS_BUCKET, TABLE_PREFIX, SRC_PREFIX, format_version=2
    )

    try:
        runner = CliRunner()
        cli_result = runner.invoke(
            migrate_app,
            [
                "--table-location",
                f"s3://{AWS_BUCKET}/{TABLE_PREFIX}",
                "--source-prefix",
                SRC_PREFIX,
                "--dest-prefix",
                f"s3://{AWS_BUCKET}/warehouse",
                "--glue-database",
                GLUE_DB,
                "--glue-table",
                GLUE_TABLE,
                "--aws-region",
                AWS_REGION,
            ],
        )
        assert cli_result.exit_code == 0, (
            f"CLI migration failed (exit {cli_result.exit_code}):\n{cli_result.output}"
        )

        cli_s3 = _snapshot_s3(aws_s3_client, AWS_BUCKET, migrated_prefix)
        cli_glue = _snapshot_glue(glue_client, GLUE_DB, GLUE_TABLE)

        event = {
            "table_location": f"s3://{AWS_BUCKET}/{TABLE_PREFIX}",
            "source_prefix": SRC_PREFIX,
            "dest_prefix": f"s3://{AWS_BUCKET}/warehouse",
            "glue_database": GLUE_DB,
            "glue_table": GLUE_TABLE,
            "aws_region": AWS_REGION,
        }
        response = lambda_client.invoke(
            FunctionName=SANDBOX_LAMBDA_FUNCTION_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps(event),
        )
        raw = response["Payload"].read()
        assert not response.get("FunctionError"), (
            f"Lambda {SANDBOX_LAMBDA_FUNCTION_NAME} returned FunctionError "
            f"{response['FunctionError']}: {raw.decode()}"
        )
        payload = json.loads(raw)
        assert payload.get("status") == "success", (
            f"Unexpected Lambda response status: {payload}"
        )

        lambda_s3 = _snapshot_s3(aws_s3_client, AWS_BUCKET, migrated_prefix)
        lambda_glue = _snapshot_glue(glue_client, GLUE_DB, GLUE_TABLE)

        assert cli_s3 == lambda_s3
        assert cli_glue == lambda_glue

    finally:
        cleanup_glue_table(glue_client, GLUE_DB, GLUE_TABLE)
        cleanup_s3_prefix(aws_s3_client, AWS_BUCKET, TABLE_PREFIX + "/")
