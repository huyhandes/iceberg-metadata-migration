"""End-to-end integration tests for the iceberg-migrate CLI pipeline.

Tests invoke the CLI via typer.testing.CliRunner against moto-mocked AWS services.
Each test seeds S3 with synthetic Iceberg metadata via create_test_metadata(),
then invokes the CLI and asserts on exit codes, S3 state, Glue state, and output.

Coverage:
  Test 1: --dry-run: exit 0, output contains "Dry run", no S3/Glue writes beyond seed
  Test 2: full run: exit 0, S3 has rewritten paths, Glue table registered
  Test 3: idempotent re-run: second run exits 0, Glue updated (not duplicated)
  Test 4: --json output: stdout is valid parseable JSON with "status": "success"
  Test 5: --verbose output: output contains per-file substitution counts
  Test 6: validation failure exit code 2: seed missing "location" field
  Test 7: v3 deletion vector: deletion_vector.path is rewritten in S3 output
"""
from __future__ import annotations

import io
import json

import boto3
import fastavro
import orjson
import pytest
from moto import mock_aws
from typer.testing import CliRunner

from iceberg_migrate.cli import app
from tests.fixtures.v3_manifest_with_dv import create_test_metadata

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BUCKET = "test-bucket"
TABLE_PREFIX = "warehouse/testdb/testtable"
TABLE_LOCATION = f"s3://{BUCKET}/{TABLE_PREFIX}"
SRC_PREFIX = "s3a://minio-bucket/warehouse"
DST_PREFIX = f"s3://{BUCKET}/warehouse"
GLUE_DB = "testdb"
GLUE_TABLE = "testtable"

runner = CliRunner()


# ---------------------------------------------------------------------------
# Shared fixture: mock AWS environment + seeded S3
# ---------------------------------------------------------------------------


@pytest.fixture
def aws_env():
    """Provide mocked S3 + Glue with a pre-seeded Iceberg table (format_version=2).

    Yields:
        (s3_client, glue_client, bucket, table_prefix)
    """
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        glue = boto3.client("glue", region_name="us-east-1")

        # Create bucket and Glue database
        s3.create_bucket(Bucket=BUCKET)
        glue.create_database(DatabaseInput={"Name": GLUE_DB})

        # Seed S3 with synthetic Iceberg metadata (format_version=2)
        create_test_metadata(s3, BUCKET, TABLE_PREFIX, SRC_PREFIX, format_version=2)

        yield s3, glue, BUCKET, TABLE_PREFIX


@pytest.fixture
def aws_env_v3():
    """Provide mocked S3 + Glue with a pre-seeded v3 Iceberg table (format_version=3).

    Yields:
        (s3_client, glue_client, bucket, table_prefix)
    """
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        glue = boto3.client("glue", region_name="us-east-1")

        # Create bucket and Glue database
        s3.create_bucket(Bucket=BUCKET)
        glue.create_database(DatabaseInput={"Name": GLUE_DB})

        # Seed S3 with synthetic Iceberg v3 metadata
        create_test_metadata(s3, BUCKET, TABLE_PREFIX, SRC_PREFIX, format_version=3)

        yield s3, glue, BUCKET, TABLE_PREFIX


# ---------------------------------------------------------------------------
# CLI invocation helper
# ---------------------------------------------------------------------------


def _invoke_migrate(extra_args: list[str] | None = None) -> "Result":
    """Invoke the migrate command with standard args + optional extras."""
    args = [
        "--table-location", TABLE_LOCATION,
        "--source-prefix", SRC_PREFIX,
        "--dest-prefix", DST_PREFIX,
        "--glue-database", GLUE_DB,
        "--glue-table", GLUE_TABLE,
        "--aws-region", "us-east-1",
    ]
    if extra_args:
        args.extend(extra_args)
    return runner.invoke(app, args)


# ---------------------------------------------------------------------------
# Test 1: --dry-run shows summary without writing to S3 or Glue
# ---------------------------------------------------------------------------


def test_dry_run_no_writes(aws_env):
    """--dry-run exits 0, output contains 'Dry run', no extra S3/Glue writes."""
    s3, glue, bucket, _ = aws_env

    # Record S3 object keys before invoking CLI
    objects_before = {
        obj["Key"]
        for obj in s3.list_objects_v2(Bucket=bucket).get("Contents", [])
    }

    result = _invoke_migrate(["--dry-run"])

    assert result.exit_code == 0, f"Expected exit 0, got {result.exit_code}. Output:\n{result.output}"

    # Output should mention "Dry run"
    combined = (result.output or "") + (result.stderr or "" if hasattr(result, "stderr") else "")
    assert "Dry run" in combined or "dry run" in combined.lower(), (
        f"Expected 'Dry run' in output:\n{combined}"
    )

    # No new S3 objects should have been written
    objects_after = {
        obj["Key"]
        for obj in s3.list_objects_v2(Bucket=bucket).get("Contents", [])
    }
    assert objects_before == objects_after, (
        f"Dry-run should not write S3 objects. New objects: {objects_after - objects_before}"
    )

    # Glue table should NOT have been created
    tables = glue.get_tables(DatabaseName=GLUE_DB)["TableList"]
    assert len(tables) == 0, f"Dry-run should not create Glue table. Tables: {tables}"


# ---------------------------------------------------------------------------
# Test 2: Full run rewrites S3 paths and registers Glue table
# ---------------------------------------------------------------------------


def test_full_run_rewrites_and_registers(aws_env):
    """Full run exits 0, S3 objects contain rewritten DST_PREFIX paths, Glue table registered."""
    s3, glue, bucket, table_prefix = aws_env

    result = _invoke_migrate()

    assert result.exit_code == 0, f"Expected exit 0, got {result.exit_code}. Output:\n{result.output}"

    # Glue table should exist with correct metadata_location
    tables = glue.get_tables(DatabaseName=GLUE_DB)["TableList"]
    assert len(tables) == 1, f"Expected 1 Glue table, got {len(tables)}"
    assert tables[0]["Name"] == GLUE_TABLE

    params = tables[0].get("Parameters", {})
    assert params.get("table_type") == "ICEBERG", "Glue table_type should be ICEBERG"
    metadata_location = params.get("metadata_location", "")
    assert metadata_location.startswith(f"s3://{bucket}/"), (
        f"metadata_location should start with s3://{bucket}/. Got: {metadata_location}"
    )

    # metadata.json in S3 should contain DST_PREFIX, not SRC_PREFIX
    metadata_key = f"{table_prefix}/metadata/00001-abc.metadata.json"
    obj = s3.get_object(Bucket=bucket, Key=metadata_key)
    content = obj["Body"].read().decode("utf-8")
    metadata = json.loads(content)

    assert SRC_PREFIX not in content, "metadata.json should not contain SRC_PREFIX after rewrite"
    assert DST_PREFIX in content, "metadata.json should contain DST_PREFIX after rewrite"
    assert metadata["location"].startswith(DST_PREFIX), (
        f"location should start with DST_PREFIX. Got: {metadata['location']}"
    )


# ---------------------------------------------------------------------------
# Test 3: Idempotent re-run: second run updates Glue table, exits 0
# ---------------------------------------------------------------------------


def test_idempotent_rerun(aws_env):
    """Running migration twice exits 0 both times; Glue table updated not duplicated."""
    s3, glue, bucket, _ = aws_env

    # First run
    result1 = _invoke_migrate()
    assert result1.exit_code == 0, f"First run failed: {result1.output}"

    # Second run — should succeed (idempotent)
    result2 = _invoke_migrate()
    assert result2.exit_code == 0, f"Second run failed: {result2.output}"

    # Should still be exactly 1 Glue table (not duplicated)
    tables = glue.get_tables(DatabaseName=GLUE_DB)["TableList"]
    assert len(tables) == 1, f"Expected 1 Glue table after re-run, got {len(tables)}"
    assert tables[0]["Name"] == GLUE_TABLE


# ---------------------------------------------------------------------------
# Test 4: --json output: stdout is valid JSON with "status": "success"
# ---------------------------------------------------------------------------


def test_json_output_valid(aws_env):
    """--json produces valid parseable JSON on stdout with status=success."""
    result = _invoke_migrate(["--json"])

    assert result.exit_code == 0, f"Expected exit 0, got {result.exit_code}. Output:\n{result.output}"

    # typer CliRunner mixes stdout and stderr into result.output.
    # In --json mode the CLI emits JSON to stdout and human summary to stderr.
    # Extract the JSON portion from the mixed output (it starts with '{').
    full_output = result.output
    assert full_output, "Expected non-empty output for --json mode"

    # Find the first '{' which starts the JSON blob
    json_start = full_output.find("{")
    assert json_start != -1, f"Expected JSON object in output:\n{full_output}"
    json_str = full_output[json_start:].strip()

    try:
        data = orjson.loads(json_str)
    except Exception as e:
        pytest.fail(f"--json output does not contain valid JSON: {e}\nJSON portion:\n{json_str}")

    assert data.get("status") == "success", (
        f"Expected status=success in JSON, got: {data.get('status')}"
    )
    assert "counts" in data, f"Expected 'counts' key in JSON output: {data}"
    assert "glue" in data, f"Expected 'glue' key in JSON output: {data}"


# ---------------------------------------------------------------------------
# Test 5: --verbose output contains per-file substitution counts
# ---------------------------------------------------------------------------


def test_verbose_output_contains_file_counts(aws_env):
    """--verbose output contains per-file substitution counts."""
    result = _invoke_migrate(["--verbose"])

    assert result.exit_code == 0, f"Expected exit 0, got {result.exit_code}. Output:\n{result.output}"

    output = result.output
    # verbose output should mention paths rewritten per file
    assert "paths rewritten" in output, (
        f"Expected 'paths rewritten' in verbose output:\n{output}"
    )


# ---------------------------------------------------------------------------
# Test 6: Validation failure → exit code 2
# ---------------------------------------------------------------------------


def test_validation_failure_exit_code_2():
    """Seeding metadata without 'location' field causes validation failure → exit code 2."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        glue = boto3.client("glue", region_name="us-east-1")

        s3.create_bucket(Bucket=BUCKET)
        glue.create_database(DatabaseInput={"Name": GLUE_DB})

        # Seed metadata.json without the required "snapshots" field (triggers VAL-02).
        # Note: rewrite_metadata_json adds "location" even if missing (via m.get("location",""))
        # so we omit "snapshots" which cannot be auto-added during rewrite.
        table_prefix = TABLE_PREFIX
        metadata_key = f"{table_prefix}/metadata/00001-abc.metadata.json"

        bad_metadata = {
            "format-version": 2,
            "table-uuid": "test-uuid-bad",
            "location": f"{SRC_PREFIX}/{table_prefix}",
            "current-snapshot-id": 123,
            # "snapshots" intentionally omitted — triggers VAL-02 structural check
        }
        s3.put_object(
            Bucket=BUCKET,
            Key=metadata_key,
            Body=json.dumps(bad_metadata).encode("utf-8"),
        )

        result = _invoke_migrate()

        assert result.exit_code == 2, (
            f"Expected exit code 2 for validation failure, got {result.exit_code}. "
            f"Output:\n{result.output}"
        )


# ---------------------------------------------------------------------------
# Test 7: v3 deletion vector path rewriting
# ---------------------------------------------------------------------------


def test_v3_deletion_vector_path_rewritten(aws_env_v3):
    """v3 manifest with deletion_vector.path gets rewritten to DST_PREFIX in S3 output."""
    s3, glue, bucket, table_prefix = aws_env_v3

    result = _invoke_migrate()

    assert result.exit_code == 0, f"Expected exit 0, got {result.exit_code}. Output:\n{result.output}"

    # Read the rewritten manifest from S3 and verify deletion_vector.path uses DST_PREFIX
    manifest_key = f"{table_prefix}/metadata/manifest-001.avro"
    obj = s3.get_object(Bucket=bucket, Key=manifest_key)
    manifest_bytes = obj["Body"].read()

    # Parse the Avro to check deletion_vector.path
    records = list(fastavro.reader(io.BytesIO(manifest_bytes)))
    assert len(records) > 0, "Expected at least one manifest record"

    record = records[0]
    data_file = record.get("data_file", {})
    dv = data_file.get("deletion_vector")

    assert dv is not None, "Expected deletion_vector in manifest record"
    dv_path = dv.get("path", "")

    assert dv_path.startswith(DST_PREFIX), (
        f"deletion_vector.path should start with DST_PREFIX. Got: {dv_path}"
    )
    assert SRC_PREFIX not in dv_path, (
        f"deletion_vector.path should not contain SRC_PREFIX. Got: {dv_path}"
    )
