"""Shared fixtures for integration tests.

Provides:
- minio_client: boto3 S3 client pointed at local MinIO
- sync_to_s3: copy objects from MinIO to real AWS S3
- athena_query: run Athena query and poll for results
- cleanup_s3: delete all objects under a prefix in AWS S3
- cleanup_glue_table: drop a Glue table
"""

from __future__ import annotations

import json
import os
import pathlib
import time
from typing import TYPE_CHECKING, Any

import boto3
import pytest

if TYPE_CHECKING:
    from mypy_boto3_athena import AthenaClient
    from mypy_boto3_emr_serverless import EMRServerlessClient
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_s3 import S3Client

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "warehouse"

AWS_BUCKET = os.environ.get("AWS_TEST_BUCKET", "")
AWS_REGION = os.environ.get("AWS_REGION", "")
GLUE_DB = os.environ.get("GLUE_DATABASE", "iceberg_migration_test")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "iceberg-migration-test")
ATHENA_TIMEOUT_SECONDS = 120

GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "iceberg-migration-verify")
EMR_APPLICATION_ID = os.environ.get("EMR_APPLICATION_ID", "")
EMR_JOB_ROLE_ARN = os.environ.get("EMR_JOB_ROLE_ARN", "")

SPARK_JOBS_DIR = pathlib.Path(__file__).parent.parent.parent / "infra" / "spark_jobs"

NAMESPACES = ["rest_ns", "sql_ns", "hms_ns"]
TABLES_TO_MIGRATE = ["sample_table", "cities"]


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


@pytest.fixture(scope="session")
def emrserverless_client() -> EMRServerlessClient:
    """Boto3 EMR Serverless client for real AWS."""
    return boto3.client("emr-serverless", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def glue_job_client() -> GlueClient:
    """Boto3 Glue client for job runs."""
    return boto3.client("glue", region_name=AWS_REGION)


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
                [col.get("VarCharValue", "") for col in row["Data"]] for row in rows[1:]
            ]
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "unknown"
            )
            raise RuntimeError(f"Athena query {state}: {reason}\nQuery: {query}")

        time.sleep(2)

    raise TimeoutError(
        f"Athena query timed out after {ATHENA_TIMEOUT_SECONDS}s: {query}"
    )


# ---------------------------------------------------------------------------
# Spark job helpers
# ---------------------------------------------------------------------------


def upload_spark_scripts(s3_client: S3Client) -> None:
    """Upload all .py files from infra/spark_jobs/ to s3://{AWS_BUCKET}/spark-jobs/.

    Called once at session start so Glue and EMR Serverless jobs can access the scripts.
    """
    for script_path in SPARK_JOBS_DIR.glob("*.py"):
        key = f"spark-jobs/{script_path.name}"
        s3_client.put_object(
            Bucket=AWS_BUCKET,
            Key=key,
            Body=script_path.read_bytes(),
        )
        print(f"Uploaded {script_path.name} to s3://{AWS_BUCKET}/{key}")


def run_glue_job(
    client: Any,
    job_name: str,
    arguments: dict[str, str],
    timeout: int = 600,
) -> dict[str, Any]:
    """Start a Glue job run and poll until SUCCEEDED or FAILED.

    Args:
        client: boto3 Glue client.
        job_name: Name of the Glue job definition.
        arguments: Job arguments dict (e.g., {"--output_path": "s3://..."}).
        timeout: Max seconds to wait.

    Returns:
        The final JobRun dict from get_job_run.

    Raises:
        RuntimeError: If the job enters a terminal failure state.
        TimeoutError: If the job does not complete within timeout seconds.
    """
    run = client.start_job_run(JobName=job_name, Arguments=arguments)
    run_id = run["JobRunId"]

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get_job_run(JobName=job_name, RunId=run_id)
        state = response["JobRun"]["JobRunState"]

        if state == "SUCCEEDED":
            return response["JobRun"]
        if state in ("FAILED", "STOPPED", "TIMEOUT", "ERROR"):
            error_msg = response["JobRun"].get("ErrorMessage", "no error message")
            raise RuntimeError(f"Glue job run {run_id} {state}: {error_msg}")

        time.sleep(15)

    raise TimeoutError(f"Glue job run {run_id} timed out after {timeout}s")


EMR_SERVERLESS_SPARK_CONF = (
    "--conf spark.sql.extensions="
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    " --conf spark.sql.catalog.glue_catalog="
    "org.apache.iceberg.spark.SparkCatalog"
    " --conf spark.sql.catalog.glue_catalog.catalog-impl="
    "org.apache.iceberg.aws.glue.GlueCatalog"
    " --conf spark.sql.catalog.glue_catalog.io-impl="
    "org.apache.iceberg.aws.s3.S3FileIO"
)


def run_emr_job(
    client: Any,
    application_id: str,
    execution_role_arn: str,
    script_s3_uri: str,
    entry_point_args: list[str],
    timeout: int = 900,
) -> dict[str, Any]:
    """Start an EMR Serverless job run and poll until SUCCESS or terminal failure.

    Args:
        client: boto3 EMR Serverless client.
        application_id: Pre-provisioned EMR Serverless application ID.
        execution_role_arn: IAM role ARN the job assumes.
        script_s3_uri: S3 URI of the PySpark entry point script.
        entry_point_args: List of string arguments passed to the script.
        timeout: Max seconds to wait.

    Returns:
        The final jobRun dict from get_job_run.

    Raises:
        RuntimeError: If the job enters a terminal failure state.
        TimeoutError: If the job does not complete within timeout seconds.
    """
    run = client.start_job_run(
        applicationId=application_id,
        executionRoleArn=execution_role_arn,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": script_s3_uri,
                "entryPointArguments": entry_point_args,
                "sparkSubmitParameters": EMR_SERVERLESS_SPARK_CONF,
            }
        },
    )
    run_id = run["jobRunId"]

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get_job_run(applicationId=application_id, jobRunId=run_id)
        state = response["jobRun"]["state"]

        if state == "SUCCESS":
            return response["jobRun"]
        if state in ("FAILED", "CANCELLED", "CANCELLING"):
            raise RuntimeError(f"EMR Serverless job run {run_id} {state}")

        time.sleep(20)

    raise TimeoutError(f"EMR Serverless job run {run_id} timed out after {timeout}s")


def read_job_results(s3_client: S3Client, output_s3_uri: str) -> dict[str, Any]:
    """Read the results.json written by a Spark verification job.

    Args:
        s3_client: boto3 S3 client.
        output_s3_uri: S3 URI prefix used as the job's output_path argument.

    Returns:
        Parsed JSON dict written by the Spark script.
    """
    without_scheme = output_s3_uri.removeprefix("s3://")
    bucket, _, prefix = without_scheme.partition("/")
    key = prefix.rstrip("/") + "/results.json"
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)


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


# ---------------------------------------------------------------------------
# Session migration fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def migrated_tables(
    minio_client: S3Client,
    aws_s3_client: S3Client,
    glue_client: GlueClient,
) -> list[tuple[str, str, str]]:
    """Session fixture: upload spark scripts, sync and migrate all tables, yield, cleanup.

    Syncs all 3 namespaces × 2 tables from MinIO to AWS S3, runs the migration CLI
    for each, and registers all 6 tables in Glue. Cleans up at session end.

    Yields:
        List of (namespace, table_name, glue_table_name) for every migrated table.
    """
    from typer.testing import CliRunner

    from iceberg_migrate.cli import app as migrate_app

    runner = CliRunner()

    upload_spark_scripts(aws_s3_client)

    migrated: list[tuple[str, str, str]] = []
    synced_namespaces: list[str] = []

    try:
        for namespace in NAMESPACES:
            count = sync_minio_to_s3(minio_client, aws_s3_client, namespace)
            assert count > 0, (
                f"No objects synced for {namespace}. Run: just seed-all"
            )
            synced_namespaces.append(namespace)

            src_prefix = f"s3://warehouse/{namespace}"
            dst_prefix = f"s3://{AWS_BUCKET}/warehouse/{namespace}"

            for table_name in TABLES_TO_MIGRATE:
                glue_table = f"{namespace}_{table_name}"
                table_location = (
                    f"s3://{AWS_BUCKET}/warehouse/{namespace}/{table_name}"
                )

                result = runner.invoke(
                    migrate_app,
                    [
                        "--table-location",
                        table_location,
                        "--source-prefix",
                        src_prefix,
                        "--dest-prefix",
                        dst_prefix,
                        "--glue-database",
                        GLUE_DB,
                        "--glue-table",
                        glue_table,
                        "--aws-region",
                        AWS_REGION,
                    ],
                )
                assert result.exit_code == 0, (
                    f"Migration failed for {namespace}.{table_name} "
                    f"(exit {result.exit_code}):\n{result.output}"
                )
                migrated.append((namespace, table_name, glue_table))

        yield migrated

    finally:
        for namespace in synced_namespaces:
            cleanup_s3_prefix(aws_s3_client, AWS_BUCKET, f"warehouse/{namespace}/")
        for _, _, glue_table in migrated:
            cleanup_glue_table(glue_client, GLUE_DB, glue_table)
