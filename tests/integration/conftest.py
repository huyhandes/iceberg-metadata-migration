"""Shared fixtures for integration tests.

Provides:
- minio_client: boto3 S3 client pointed at local MinIO
- sync_to_s3: copy objects from MinIO to real AWS S3
- athena_query: run Athena query and poll for results
- cleanup_s3: delete all objects under a prefix in AWS S3
- cleanup_glue_table: drop a Glue table
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import boto3
import pytest

if TYPE_CHECKING:
    from mypy_boto3_athena import AthenaClient
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_s3 import S3Client

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "warehouse"

AWS_BUCKET = "YOUR_TEST_BUCKET"
AWS_REGION = "YOUR_REGION"
GLUE_DB = "iceberg_migration_test"
ATHENA_WORKGROUP = "iceberg-migration-test"
ATHENA_TIMEOUT_SECONDS = 120


# ---------------------------------------------------------------------------
# MinIO client
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def minio_client() -> S3Client:
    """Boto3 S3 client pointed at local MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def aws_s3_client() -> S3Client:
    """Boto3 S3 client for real AWS (uses AWS_PROFILE from env)."""
    return boto3.client("s3", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def glue_client() -> GlueClient:
    """Boto3 Glue client for real AWS."""
    return boto3.client("glue", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def athena_client() -> AthenaClient:
    """Boto3 Athena client for real AWS."""
    return boto3.client("athena", region_name=AWS_REGION)


# ---------------------------------------------------------------------------
# Sync: MinIO -> AWS S3
# ---------------------------------------------------------------------------


def sync_minio_to_s3(
    minio: S3Client,
    aws_s3: S3Client,
    namespace: str,
) -> int:
    """Copy all objects for a namespace from MinIO to AWS S3.

    Downloads from MinIO, uploads to AWS S3 under warehouse/{namespace}/.
    Returns the number of objects synced.
    """
    prefix = f"{namespace}/"
    paginator = minio.get_paginator("list_objects_v2")
    count = 0

    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            body = minio.get_object(Bucket=MINIO_BUCKET, Key=key)["Body"].read()
            aws_s3.put_object(
                Bucket=AWS_BUCKET,
                Key=f"warehouse/{key}",
                Body=body,
            )
            count += 1

    return count


# ---------------------------------------------------------------------------
# Athena query helper
# ---------------------------------------------------------------------------


def run_athena_query(
    client: AthenaClient,
    query: str,
    database: str = GLUE_DB,
) -> list[list[str]]:
    """Execute an Athena query and return result rows (excluding header).

    Polls until SUCCEEDED/FAILED. Raises on failure or timeout.
    """
    execution = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=ATHENA_WORKGROUP,
    )
    execution_id = execution["QueryExecutionId"]

    # Poll for completion
    deadline = time.monotonic() + ATHENA_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        status = client.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            results = client.get_query_results(QueryExecutionId=execution_id)
            rows = results["ResultSet"]["Rows"]
            # First row is header, skip it
            return [
                [col.get("VarCharValue", "") for col in row["Data"]]
                for row in rows[1:]
            ]
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "unknown"
            )
            raise RuntimeError(
                f"Athena query {state}: {reason}\nQuery: {query}"
            )

        time.sleep(2)

    raise TimeoutError(
        f"Athena query timed out after {ATHENA_TIMEOUT_SECONDS}s: {query}"
    )


# ---------------------------------------------------------------------------
# Cleanup helpers
# ---------------------------------------------------------------------------


def cleanup_s3_prefix(client: S3Client, bucket: str, prefix: str) -> int:
    """Delete all objects under a prefix. Returns count deleted."""
    paginator = client.get_paginator("list_objects_v2")
    count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            count += len(objects)

    return count


def cleanup_glue_table(client: GlueClient, database: str, table: str) -> None:
    """Delete a Glue table, ignoring if it doesn't exist."""
    try:
        client.delete_table(DatabaseName=database, Name=table)
    except client.exceptions.EntityNotFoundException:
        pass
