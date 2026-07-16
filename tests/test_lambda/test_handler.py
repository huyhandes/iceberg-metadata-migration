"""Unit tests for the Lambda migration handler (issue #7).

Mirrors tests/test_integration/test_end_to_end.py fixture/invocation style:
same constants, mock_aws + create_test_metadata seeding, create_bucket +
create_database setup. Invokes :func:`handler` directly (no CliRunner).

Cases (per issue #7):
  1. valid event -> success dict (counts/glue keys; Glue table created; S3 rewritten)
  2. missing required field -> raises before any S3 write
  3. Glue failure -> raises (DB not created -> register_or_update fails)
  4. dry_run -> success, no S3 writes, no Glue table
"""

from __future__ import annotations

import json

import boto3
import pytest
from moto import mock_aws

from iceberg_migrate.lambda_handler import handler
from tests.fixtures.v3_manifest_with_dv import create_test_metadata

# ---------------------------------------------------------------------------
# Constants (mirror test_end_to_end.py)
# ---------------------------------------------------------------------------

BUCKET = "test-bucket"
TABLE_PREFIX = "warehouse/testdb/testtable"
TABLE_LOCATION = f"s3://{BUCKET}/{TABLE_PREFIX}"
SRC_PREFIX = "s3a://minio-bucket/warehouse"
DST_PREFIX = f"s3://{BUCKET}/warehouse"
GLUE_DB = "testdb"
GLUE_TABLE = "testtable"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def aws_env():
    """mock_aws S3+Glue, bucket + Glue DB created, Iceberg metadata seeded (v2)."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        glue = boto3.client("glue", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        glue.create_database(DatabaseInput={"Name": GLUE_DB})
        create_test_metadata(s3, BUCKET, TABLE_PREFIX, SRC_PREFIX, format_version=2)
        yield s3, glue


def _event(**overrides) -> dict:
    """Build a valid event with optional overrides (drop a key via overrides=None)."""
    base = {
        "table_location": TABLE_LOCATION,
        "source_prefix": SRC_PREFIX,
        "dest_prefix": DST_PREFIX,
        "glue_database": GLUE_DB,
        "glue_table": GLUE_TABLE,
        "aws_region": "us-east-1",
    }
    base.update(overrides)
    return base


def _s3_keys(s3) -> set[str]:
    return {obj["Key"] for obj in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])}


# ---------------------------------------------------------------------------
# Test 1: valid event -> success dict
# ---------------------------------------------------------------------------


def test_valid_event_returns_success(aws_env):
    s3, glue = aws_env
    result = handler(_event(), {})

    assert result["status"] == "success"
    assert result["dry_run"] is False
    assert result["source_prefix"] == SRC_PREFIX
    assert result["dest_prefix"] == DST_PREFIX
    assert result["table_location"] == TABLE_LOCATION
    assert "metadata_s3_key" in result and result["metadata_s3_key"]
    assert "counts" in result
    counts = result["counts"]
    assert counts["manifests_written"] >= 1
    assert counts["manifest_lists_written"] >= 1
    assert counts["metadata_written"] >= 1
    assert counts["paths_rewritten"] >= 1
    assert result["glue"] == {
        "database": GLUE_DB,
        "table": GLUE_TABLE,
        "action": "created",
    }
    assert "duration_seconds" in result

    # Glue table created (mirror e2e test 2)
    tables = glue.get_tables(DatabaseName=GLUE_DB)["TableList"]
    assert len(tables) == 1
    assert tables[0]["Name"] == GLUE_TABLE
    assert tables[0]["Parameters"].get("table_type") == "ICEBERG"

    # S3 metadata contains DST_PREFIX, not SRC_PREFIX (mirror e2e test 2)
    metadata_key = f"{TABLE_PREFIX}/_migrated/metadata/00001-abc.metadata.json"
    content = s3.get_object(Bucket=BUCKET, Key=metadata_key)["Body"].read().decode()
    assert DST_PREFIX in content
    assert SRC_PREFIX not in content
    assert json.loads(content)["location"].startswith(DST_PREFIX)


# ---------------------------------------------------------------------------
# Test 2: missing required field -> raises before any S3 write
# ---------------------------------------------------------------------------


def test_missing_required_field_raises(aws_env):
    s3, _ = aws_env
    before = _s3_keys(s3)

    bad_event = _event()
    del bad_event["table_location"]

    with pytest.raises(Exception):
        handler(bad_event, {})

    # No _migrated writes occurred (raise happened before any work)
    after = _s3_keys(s3)
    assert before == after
    assert not any(k.startswith(f"{TABLE_PREFIX}/_migrated") for k in after)


# ---------------------------------------------------------------------------
# Test 3: Glue failure -> raises
# ---------------------------------------------------------------------------


def test_glue_failure_raises():
    """Seed S3 but DON'T create the Glue database -> register_or_update fails.

    register_or_update does not create databases (glue_registrar.py Pitfall 3),
    so create_table on a missing DB raises EntityNotFoundException, which core
    surfaces as PartialMigrationError (S3 writes completed first).
    """
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        # Intentionally NO glue.create_database(...) -> registration fails.
        create_test_metadata(s3, BUCKET, TABLE_PREFIX, SRC_PREFIX, format_version=2)

        with pytest.raises(Exception):
            handler(_event(), {})


# ---------------------------------------------------------------------------
# Test 4: dry_run -> success, no writes, no Glue table
# ---------------------------------------------------------------------------


def test_dry_run_no_writes(aws_env):
    s3, glue = aws_env
    before = _s3_keys(s3)

    result = handler(_event(dry_run=True), {})

    assert result["status"] == "success"
    assert result["dry_run"] is True
    assert result["glue"]["action"] is None

    # No new S3 objects (mirror e2e test 1)
    assert before == _s3_keys(s3)

    # No Glue table created (mirror e2e test 1)
    assert glue.get_tables(DatabaseName=GLUE_DB)["TableList"] == []
