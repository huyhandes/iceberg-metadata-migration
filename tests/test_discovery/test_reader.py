"""Tests for the metadata graph reader — load_avro_with_schema and load_metadata_graph."""

import io
import json

import fastavro

from iceberg_migrate.discovery.reader import load_avro_with_schema, load_metadata_graph
from iceberg_migrate.models import IcebergMetadataGraph


BUCKET = "test-bucket"
TABLE_PREFIX = "warehouse/db/table"


# ---------------------------------------------------------------------------
# Avro fixture helpers
# ---------------------------------------------------------------------------


def make_manifest_list_avro(manifest_paths: list[str]) -> bytes:
    """Create minimal valid Avro bytes for a manifest list file."""
    schema = {
        "type": "record",
        "name": "manifest_file",
        "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "manifest_length", "type": "long"},
        ],
    }
    records = [{"manifest_path": p, "manifest_length": 1000} for p in manifest_paths]
    buf = io.BytesIO()
    fastavro.writer(buf, schema, records)
    return buf.getvalue()


def make_manifest_avro(file_paths: list[str]) -> bytes:
    """Create minimal valid Avro bytes for a manifest file."""
    schema = {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int"},
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                        {"name": "file_path", "type": "string"},
                        {"name": "file_size_in_bytes", "type": "long"},
                    ],
                },
            },
        ],
    }
    records = [
        {"status": 1, "data_file": {"file_path": p, "file_size_in_bytes": 1024}}
        for p in file_paths
    ]
    buf = io.BytesIO()
    fastavro.writer(buf, schema, records)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# S3 upload helpers
# ---------------------------------------------------------------------------


def upload_bytes(s3_client, key: str, data: bytes) -> None:
    s3_client.put_object(Bucket=BUCKET, Key=key, Body=data)


def setup_bucket(s3_client) -> None:
    s3_client.create_bucket(Bucket=BUCKET)


# ---------------------------------------------------------------------------
# Test 1: load_avro_with_schema returns (writer_schema, records)
# ---------------------------------------------------------------------------
def test_load_avro_with_schema_returns_schema_and_records():
    """load_avro_with_schema returns a (dict, list) tuple with non-empty schema."""
    avro_bytes = make_manifest_list_avro(["s3://bucket/metadata/manifest-1.avro"])
    schema, records = load_avro_with_schema(avro_bytes)
    assert isinstance(schema, dict), "writer_schema should be a dict"
    assert len(schema) > 0, "writer_schema should not be empty"
    assert isinstance(records, list)
    assert len(records) == 1
    assert records[0]["manifest_path"] == "s3://bucket/metadata/manifest-1.avro"


# ---------------------------------------------------------------------------
# Test 2: load_metadata_graph basic happy path
# ---------------------------------------------------------------------------
def test_load_metadata_graph_basic(s3_client):
    """load_metadata_graph returns populated IcebergMetadataGraph."""
    setup_bucket(s3_client)

    manifest_list_key = f"{TABLE_PREFIX}/metadata/snap-1-manifest-list.avro"
    manifest_key = f"{TABLE_PREFIX}/metadata/manifest-1.avro"

    manifest_avro = make_manifest_avro(["s3://test-bucket/data/file1.parquet"])
    manifest_list_avro = make_manifest_list_avro([f"s3://{BUCKET}/{manifest_key}"])

    metadata = {
        "format-version": 2,
        "table-uuid": "test-uuid-1234",
        "location": f"s3://{BUCKET}/{TABLE_PREFIX}",
        "current-snapshot-id": 1,
        "snapshots": [
            {
                "snapshot-id": 1,
                "timestamp-ms": 1700000000000,
                "manifest-list": f"s3://{BUCKET}/{manifest_list_key}",
                "summary": {"operation": "append"},
            }
        ],
    }

    upload_bytes(
        s3_client,
        f"{TABLE_PREFIX}/metadata/v1.metadata.json",
        json.dumps(metadata).encode(),
    )
    upload_bytes(s3_client, manifest_list_key, manifest_list_avro)
    upload_bytes(s3_client, manifest_key, manifest_avro)

    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)

    assert isinstance(graph, IcebergMetadataGraph)
    assert graph.metadata["format-version"] == 2
    assert len(graph.manifest_lists) == 1
    assert len(graph.manifests) == 1
    assert graph.manifest_lists[0].s3_key == manifest_list_key
    assert graph.manifests[0].s3_key == manifest_key


# ---------------------------------------------------------------------------
# Test 3: s3a:// manifest paths are resolved by filename extraction
# ---------------------------------------------------------------------------
def test_load_metadata_graph_resolves_s3a_paths(s3_client):
    """s3a:// manifest paths are resolved by extracting filename from the table prefix."""
    setup_bucket(s3_client)

    manifest_list_key = f"{TABLE_PREFIX}/metadata/snap-1-manifest-list.avro"
    manifest_key = f"{TABLE_PREFIX}/metadata/manifest-1.avro"

    # Manifest list record points to s3a:// (old MinIO path)
    manifest_list_avro = make_manifest_list_avro(
        [f"s3a://source-bucket/{TABLE_PREFIX}/metadata/manifest-1.avro"]
    )
    manifest_avro = make_manifest_avro(["s3a://source-bucket/data/part-0.parquet"])

    metadata = {
        "format-version": 2,
        "table-uuid": "test-uuid-5678",
        "location": f"s3a://source-bucket/{TABLE_PREFIX}",
        "current-snapshot-id": 42,
        "snapshots": [
            {
                "snapshot-id": 42,
                "timestamp-ms": 1700000000000,
                # Manifest list URI uses s3a:// — must be resolved by filename
                "manifest-list": f"s3a://source-bucket/{TABLE_PREFIX}/metadata/snap-1-manifest-list.avro",
                "summary": {"operation": "append"},
            }
        ],
    }

    upload_bytes(
        s3_client,
        f"{TABLE_PREFIX}/metadata/v1.metadata.json",
        json.dumps(metadata).encode(),
    )
    upload_bytes(s3_client, manifest_list_key, manifest_list_avro)
    upload_bytes(s3_client, manifest_key, manifest_avro)

    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)

    assert len(graph.manifest_lists) == 1
    assert len(graph.manifests) == 1
    # Keys must be in the target bucket, not the s3a:// source bucket
    assert graph.manifest_lists[0].s3_key == manifest_list_key
    assert graph.manifests[0].s3_key == manifest_key


# ---------------------------------------------------------------------------
# Test 4: writer_schema is preserved in avro_schema fields
# ---------------------------------------------------------------------------
def test_avro_schema_preserved_in_graph(s3_client):
    """avro_schema on ManifestListFile and ManifestFile is a non-empty dict."""
    setup_bucket(s3_client)

    manifest_list_key = f"{TABLE_PREFIX}/metadata/snap-1-manifest-list.avro"
    manifest_key = f"{TABLE_PREFIX}/metadata/manifest-1.avro"

    manifest_list_avro = make_manifest_list_avro([f"s3://{BUCKET}/{manifest_key}"])
    manifest_avro = make_manifest_avro(["s3://bucket/data/file.parquet"])

    metadata = {
        "format-version": 2,
        "table-uuid": "test-uuid-9999",
        "location": f"s3://{BUCKET}/{TABLE_PREFIX}",
        "current-snapshot-id": 7,
        "snapshots": [
            {
                "snapshot-id": 7,
                "timestamp-ms": 1700000000000,
                "manifest-list": f"s3://{BUCKET}/{manifest_list_key}",
                "summary": {"operation": "append"},
            }
        ],
    }

    upload_bytes(
        s3_client,
        f"{TABLE_PREFIX}/metadata/v1.metadata.json",
        json.dumps(metadata).encode(),
    )
    upload_bytes(s3_client, manifest_list_key, manifest_list_avro)
    upload_bytes(s3_client, manifest_key, manifest_avro)

    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)

    assert isinstance(graph.manifest_lists[0].avro_schema, dict)
    assert len(graph.manifest_lists[0].avro_schema) > 0
    assert isinstance(graph.manifests[0].avro_schema, dict)
    assert len(graph.manifests[0].avro_schema) > 0


# ---------------------------------------------------------------------------
# Test 5: no current-snapshot-id returns empty graph
# ---------------------------------------------------------------------------
def test_no_current_snapshot_returns_empty_graph(s3_client):
    """load_metadata_graph returns empty manifest_lists and manifests when no snapshot."""
    setup_bucket(s3_client)

    metadata = {
        "format-version": 2,
        "table-uuid": "test-uuid-empty",
        "location": f"s3://{BUCKET}/{TABLE_PREFIX}",
    }

    upload_bytes(
        s3_client,
        f"{TABLE_PREFIX}/metadata/v1.metadata.json",
        json.dumps(metadata).encode(),
    )

    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)

    assert isinstance(graph, IcebergMetadataGraph)
    assert graph.manifest_lists == []
    assert graph.manifests == []
    assert graph.metadata["table-uuid"] == "test-uuid-empty"
