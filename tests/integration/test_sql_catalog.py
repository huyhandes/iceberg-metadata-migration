"""Local integration test: migrate Iceberg table seeded via SQLite SQL catalog.

Requires:
  - docker compose up -d (MinIO only)
  - uv run python infra/seed/seed_sql.py

Verifies the migration tool correctly rewrites paths in metadata produced
by a SQL catalog (SQLite).
"""

from __future__ import annotations

import pytest

from tests.integration.conftest import MINIO_BUCKET, MINIO_ENDPOINT

NAMESPACE = "sql_ns"
TABLE_NAME = "sample_table"
SRC_PREFIX = f"s3://warehouse/{NAMESPACE}"
DST_PREFIX = f"s3://target-bucket/migrated/{NAMESPACE}"


@pytest.mark.sql
def test_sql_metadata_exists_on_minio(minio_client):
    """Verify seed script has written metadata to MinIO."""
    prefix = f"{NAMESPACE}/{TABLE_NAME}/metadata/"
    response = minio_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
    objects = response.get("Contents", [])
    assert len(objects) > 0, (
        f"No metadata objects found at {prefix}. "
        "Did you run: uv run python infra/seed/seed_sql.py?"
    )
    keys = [obj["Key"] for obj in objects]
    has_metadata_json = any(k.endswith(".metadata.json") for k in keys)
    has_avro = any(k.endswith(".avro") for k in keys)
    assert has_metadata_json, f"No metadata.json found in {keys}"
    assert has_avro, f"No Avro manifest files found in {keys}"


@pytest.mark.sql
def test_sql_migration_rewrites_paths(minio_client):
    """Run migration CLI against SQL-seeded metadata and verify path rewriting."""
    from typer.testing import CliRunner
    from iceberg_migrate.cli import app

    prefix = f"{NAMESPACE}/{TABLE_NAME}/metadata/"
    response = minio_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
    objects = response.get("Contents", [])
    metadata_keys = sorted(
        [obj["Key"] for obj in objects if obj["Key"].endswith(".metadata.json")]
    )
    assert metadata_keys, "No metadata.json found on MinIO"

    table_location = f"s3://{MINIO_BUCKET}/{NAMESPACE}/{TABLE_NAME}"

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "--table-location",
            table_location,
            "--source-prefix",
            SRC_PREFIX,
            "--dest-prefix",
            DST_PREFIX,
            "--dry-run",
            "--aws-region",
            "us-east-1",
        ],
        env={
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
        },
    )

    assert result.exit_code == 0, (
        f"CLI dry-run failed with exit {result.exit_code}.\nOutput:\n{result.output}"
    )
    output_lower = result.output.lower()
    assert "dry run" in output_lower or "paths rewritten" in output_lower, (
        f"Expected dry-run or rewrite info in output:\n{result.output}"
    )
