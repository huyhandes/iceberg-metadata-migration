"""Tests for the S3 writer module (write_all bottom-up S3 writes).

Tests:
  1. write_all writes manifests, manifest lists, then metadata (all objects in S3 after call)
  2. write_all returns WriteResult with correct counts
  3. write_all with empty manifest/manifest_list dicts still writes metadata.json
  4. write_all raises on S3 put_object failure (no silent swallowing)
"""
import boto3
import pytest
from moto import mock_aws

from iceberg_migrate.models import IcebergMetadataGraph
from iceberg_migrate.rewrite.engine import RewriteResult
from iceberg_migrate.writer.s3_writer import WriteResult, write_all

BUCKET = "test-bucket"
METADATA_KEY = "warehouse/db/table/metadata/v1.metadata.json"


def _make_result(
    manifest_bytes: dict[str, bytes] | None = None,
    manifest_list_bytes: dict[str, bytes] | None = None,
    metadata_bytes: bytes = b'{"format-version": 2}',
    metadata_s3_key: str = METADATA_KEY,
) -> RewriteResult:
    """Build a minimal RewriteResult for testing."""
    graph = IcebergMetadataGraph(
        metadata_s3_key=metadata_s3_key,
        metadata={"format-version": 2},
        manifest_lists=[],
        manifests=[],
    )
    return RewriteResult(
        graph=graph,
        metadata_bytes=metadata_bytes,
        manifest_list_bytes=manifest_list_bytes or {},
        manifest_bytes=manifest_bytes or {},
    )


@pytest.fixture
def s3(s3_client):
    """Create the test bucket and return the client."""
    s3_client.create_bucket(Bucket=BUCKET)
    return s3_client


def test_write_all_writes_all_objects(s3):
    """Test 1: All objects written to S3 after write_all call."""
    manifest_bytes = {
        "warehouse/db/table/metadata/m1.avro": b"manifest-1",
        "warehouse/db/table/metadata/m2.avro": b"manifest-2",
    }
    manifest_list_bytes = {
        "warehouse/db/table/metadata/snap-1.avro": b"manifest-list-1",
    }
    result = _make_result(
        manifest_bytes=manifest_bytes,
        manifest_list_bytes=manifest_list_bytes,
    )

    write_all(s3, BUCKET, result)

    # Verify all manifest objects exist
    for key in manifest_bytes:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        assert obj["Body"].read() == manifest_bytes[key]

    # Verify manifest list objects exist
    for key in manifest_list_bytes:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        assert obj["Body"].read() == manifest_list_bytes[key]

    # Verify metadata.json exists
    obj = s3.get_object(Bucket=BUCKET, Key=METADATA_KEY)
    assert obj["Body"].read() == b'{"format-version": 2}'


def test_write_all_returns_correct_counts(s3):
    """Test 2: write_all returns WriteResult with correct counts."""
    manifest_bytes = {
        "warehouse/db/table/metadata/m1.avro": b"manifest-1",
        "warehouse/db/table/metadata/m2.avro": b"manifest-2",
        "warehouse/db/table/metadata/m3.avro": b"manifest-3",
    }
    manifest_list_bytes = {
        "warehouse/db/table/metadata/snap-1.avro": b"manifest-list-1",
        "warehouse/db/table/metadata/snap-2.avro": b"manifest-list-2",
    }
    result = _make_result(
        manifest_bytes=manifest_bytes,
        manifest_list_bytes=manifest_list_bytes,
    )

    write_result = write_all(s3, BUCKET, result)

    assert isinstance(write_result, WriteResult)
    assert write_result.manifests_written == 3
    assert write_result.manifest_lists_written == 2
    assert write_result.metadata_written == 1
    assert write_result.metadata_s3_key == METADATA_KEY


def test_write_all_empty_manifests_writes_metadata(s3):
    """Test 3: write_all with empty manifest dicts still writes metadata.json."""
    result = _make_result(
        manifest_bytes={},
        manifest_list_bytes={},
        metadata_bytes=b'{"format-version": 3}',
    )

    write_result = write_all(s3, BUCKET, result)

    assert write_result.manifests_written == 0
    assert write_result.manifest_lists_written == 0
    assert write_result.metadata_written == 1
    obj = s3.get_object(Bucket=BUCKET, Key=METADATA_KEY)
    assert obj["Body"].read() == b'{"format-version": 3}'


def test_write_all_raises_on_s3_failure():
    """Test 4: write_all raises on S3 put_object failure (no silent swallowing)."""
    with mock_aws():
        bad_client = boto3.client("s3", region_name="us-east-1")
        # Do NOT create the bucket — any put_object call should raise
        result = _make_result(
            manifest_bytes={"warehouse/db/table/m1.avro": b"data"},
        )
        with pytest.raises(Exception):
            write_all(bad_client, "nonexistent-bucket", result)
