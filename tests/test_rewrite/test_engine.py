"""Tests for RewriteEngine orchestrating full path rewrite across all metadata layers."""

import io
import json

import fastavro
import pytest

from iceberg_migrate.discovery.reader import load_avro_with_schema, load_metadata_graph
from iceberg_migrate.rewrite.config import RewriteConfig
from iceberg_migrate.rewrite.engine import RewriteEngine, RewriteResult


BUCKET = "test-bucket"
TABLE_PREFIX = "warehouse/db/table"
SRC_PREFIX = f"s3a://minio-bucket/{TABLE_PREFIX}"
DST_PREFIX = f"s3://aws-bucket/{TABLE_PREFIX}"

CONFIG = RewriteConfig(
    src_prefix="s3a://minio-bucket",
    dst_prefix="s3://aws-bucket",
)


def migrated_key(key: str) -> str:
    """Convert a key to its _migrated/ equivalent."""
    return key.replace(TABLE_PREFIX + "/", TABLE_PREFIX + "/_migrated/", 1)


# ---------------------------------------------------------------------------
# Avro fixture helpers
# ---------------------------------------------------------------------------


def make_manifest_list_avro(manifest_paths: list[str]) -> bytes:
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


def upload_bytes(s3_client, key: str, data: bytes) -> None:
    s3_client.put_object(Bucket=BUCKET, Key=key, Body=data)


def setup_bucket(s3_client) -> None:
    s3_client.create_bucket(Bucket=BUCKET)


def build_simple_table(
    s3_client,
    snapshot_id=1,
    ml_filename="snap-1.avro",
    manifest_filename="manifest-1.avro",
    src_prefix="s3a://minio-bucket",
):
    """Set up a simple table with one snapshot and return metadata."""
    ml_key = f"{TABLE_PREFIX}/metadata/{ml_filename}"
    manifest_key = f"{TABLE_PREFIX}/metadata/{manifest_filename}"
    data_path = f"{src_prefix}/{TABLE_PREFIX}/data/part-0.parquet"

    upload_bytes(s3_client, manifest_key, make_manifest_avro([data_path]))
    upload_bytes(
        s3_client, ml_key, make_manifest_list_avro([f"s3://{BUCKET}/{manifest_key}"])
    )

    metadata = {
        "format-version": 2,
        "location": f"{src_prefix}/{TABLE_PREFIX}",
        "current-snapshot-id": snapshot_id,
        "snapshots": [
            {
                "snapshot-id": snapshot_id,
                "manifest-list": f"s3://{BUCKET}/{ml_key}",
                "summary": {"operation": "append"},
            },
        ],
    }
    upload_bytes(
        s3_client,
        f"{TABLE_PREFIX}/metadata/v1.metadata.json",
        json.dumps(metadata).encode(),
    )
    return metadata, ml_key, manifest_key


# ---------------------------------------------------------------------------
# Test 1: RewriteEngine rewrites metadata.json location
# ---------------------------------------------------------------------------
def test_rewrite_engine_rewrites_location(s3_client):
    """RewriteEngine.rewrite rewrites metadata location from src to dst prefix."""
    setup_bucket(s3_client)
    build_simple_table(s3_client)
    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)

    engine = RewriteEngine(CONFIG)
    result = engine.rewrite(graph, s3_client, BUCKET, TABLE_PREFIX)

    assert isinstance(result, RewriteResult)
    assert result.graph.metadata["location"] == f"s3://aws-bucket/{TABLE_PREFIX}"


# ---------------------------------------------------------------------------
# Test 2: RewriteEngine calls load_full_graph — ALL snapshots' manifest lists rewritten
# ---------------------------------------------------------------------------
def test_rewrite_engine_loads_all_snapshots(s3_client):
    """RewriteEngine calls load_full_graph before rewriting so ALL snapshots are covered.

    Phase 1 graph only has current snapshot's manifest list. RewriteEngine must
    use load_full_graph to ensure historical snapshot manifest lists are also rewritten.
    """
    setup_bucket(s3_client)

    ml_key_1 = f"{TABLE_PREFIX}/metadata/snap-1-manifest-list.avro"
    ml_key_2 = f"{TABLE_PREFIX}/metadata/snap-2-manifest-list.avro"
    manifest_key_1 = f"{TABLE_PREFIX}/metadata/manifest-1.avro"
    manifest_key_2 = f"{TABLE_PREFIX}/metadata/manifest-2.avro"

    upload_bytes(
        s3_client,
        manifest_key_1,
        make_manifest_avro([f"s3a://minio-bucket/{TABLE_PREFIX}/data/part-0.parquet"]),
    )
    upload_bytes(
        s3_client,
        manifest_key_2,
        make_manifest_avro([f"s3a://minio-bucket/{TABLE_PREFIX}/data/part-1.parquet"]),
    )
    upload_bytes(
        s3_client,
        ml_key_1,
        make_manifest_list_avro([f"s3://{BUCKET}/{manifest_key_1}"]),
    )
    upload_bytes(
        s3_client,
        ml_key_2,
        make_manifest_list_avro([f"s3://{BUCKET}/{manifest_key_2}"]),
    )

    metadata = {
        "format-version": 2,
        "location": f"s3a://minio-bucket/{TABLE_PREFIX}",
        "current-snapshot-id": 2,
        "snapshots": [
            {
                "snapshot-id": 1,
                "manifest-list": f"s3://{BUCKET}/{ml_key_1}",
            },
            {
                "snapshot-id": 2,
                "manifest-list": f"s3://{BUCKET}/{ml_key_2}",
            },
        ],
    }
    upload_bytes(
        s3_client,
        f"{TABLE_PREFIX}/metadata/v1.metadata.json",
        json.dumps(metadata).encode(),
    )

    # Phase 1 graph only loads current snapshot (snapshot 2)
    phase1_graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)
    assert len(phase1_graph.manifest_lists) == 1

    engine = RewriteEngine(CONFIG)
    result = engine.rewrite(phase1_graph, s3_client, BUCKET, TABLE_PREFIX)

    # Both manifest lists should be present in the rewritten graph
    assert len(result.graph.manifest_lists) == 2
    assert len(result.graph.manifests) == 2

    # Both manifest list bytes should be in the output
    assert len(result.manifest_list_bytes) == 2
    assert len(result.manifest_bytes) == 2


# ---------------------------------------------------------------------------
# Test 3: RewriteEngine returns RewriteResult with rewritten graph and bytes
# ---------------------------------------------------------------------------
def test_rewrite_engine_returns_rewrite_result(s3_client):
    """RewriteEngine.rewrite returns a RewriteResult with graph, metadata_bytes, Avro bytes."""
    setup_bucket(s3_client)
    build_simple_table(s3_client)
    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)

    engine = RewriteEngine(CONFIG)
    result = engine.rewrite(graph, s3_client, BUCKET, TABLE_PREFIX)

    assert isinstance(result, RewriteResult)
    assert isinstance(result.metadata_bytes, bytes)
    assert len(result.metadata_bytes) > 0
    assert isinstance(result.manifest_list_bytes, dict)
    assert isinstance(result.manifest_bytes, dict)
    assert len(result.manifest_list_bytes) == 1
    assert len(result.manifest_bytes) == 1


# ---------------------------------------------------------------------------
# Test 4: Avro round-trip for manifest list (fastavro serialization preserves schema)
# ---------------------------------------------------------------------------
def test_rewrite_engine_manifest_list_avro_roundtrip(s3_client):
    """RewriteEngine serializes rewritten manifest list records back to valid Avro bytes.

    Round-trip: read original -> rewrite -> serialize -> read back -> verify paths changed.
    """
    setup_bucket(s3_client)
    _, ml_key, _ = build_simple_table(s3_client)
    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)

    engine = RewriteEngine(CONFIG)
    result = engine.rewrite(graph, s3_client, BUCKET, TABLE_PREFIX)

    assert migrated_key(ml_key) in result.manifest_list_bytes
    rewritten_bytes = result.manifest_list_bytes[migrated_key(ml_key)]

    # Read back the serialized Avro bytes
    schema, records, codec = load_avro_with_schema(rewritten_bytes)
    assert len(records) == 1
    # manifest_path should NOT contain s3a://minio-bucket anymore
    assert "s3a://minio-bucket" not in records[0]["manifest_path"]


# ---------------------------------------------------------------------------
# Test 5: Avro round-trip for manifest (fastavro serialization preserves schema)
# ---------------------------------------------------------------------------
def test_rewrite_engine_manifest_avro_roundtrip(s3_client):
    """RewriteEngine serializes rewritten manifest records back to valid Avro bytes.

    Round-trip: read original -> rewrite -> serialize -> read back -> verify paths changed.
    """
    setup_bucket(s3_client)
    _, _, manifest_key = build_simple_table(s3_client)
    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)

    engine = RewriteEngine(CONFIG)
    result = engine.rewrite(graph, s3_client, BUCKET, TABLE_PREFIX)

    assert migrated_key(manifest_key) in result.manifest_bytes
    rewritten_bytes = result.manifest_bytes[migrated_key(manifest_key)]

    # Read back the serialized Avro bytes
    schema, records, codec = load_avro_with_schema(rewritten_bytes)
    assert len(records) == 1
    # file_path should NOT contain s3a://minio-bucket anymore
    assert "s3a://minio-bucket" not in records[0]["data_file"]["file_path"]
    # Should be rewritten to aws-bucket
    assert "s3://aws-bucket" in records[0]["data_file"]["file_path"]


# ---------------------------------------------------------------------------
# Test 6: Original graph is not mutated after rewrite
# ---------------------------------------------------------------------------
def test_rewrite_engine_does_not_mutate_original_graph(s3_client):
    """Original graph is not mutated after RewriteEngine.rewrite."""
    setup_bucket(s3_client)
    build_simple_table(s3_client)
    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)

    original_location = graph.metadata["location"]
    original_ml_count = len(graph.manifest_lists)

    engine = RewriteEngine(CONFIG)
    result = engine.rewrite(graph, s3_client, BUCKET, TABLE_PREFIX)

    # Original graph's metadata location should be unchanged
    assert graph.metadata["location"] == original_location
    assert len(graph.manifest_lists) == original_ml_count
    # Result should be rewritten
    assert result.graph.metadata["location"] != original_location


# ---------------------------------------------------------------------------
# Test 7: Table with no snapshots returns rewritten metadata but empty Avro bytes
# ---------------------------------------------------------------------------
def test_rewrite_engine_no_snapshots(s3_client):
    """Table with no snapshots: RewriteEngine rewrites metadata but has empty Avro byte maps."""
    setup_bucket(s3_client)

    metadata = {
        "format-version": 2,
        "location": f"s3a://minio-bucket/{TABLE_PREFIX}",
    }
    upload_bytes(
        s3_client,
        f"{TABLE_PREFIX}/metadata/v1.metadata.json",
        json.dumps(metadata).encode(),
    )

    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)
    assert len(graph.manifest_lists) == 0

    engine = RewriteEngine(CONFIG)
    result = engine.rewrite(graph, s3_client, BUCKET, TABLE_PREFIX)

    assert result.graph.metadata["location"] == f"s3://aws-bucket/{TABLE_PREFIX}"
    assert result.manifest_list_bytes == {}
    assert result.manifest_bytes == {}
    assert isinstance(result.metadata_bytes, bytes)


def test_rewrite_engine_rejects_unknown_format_version(s3_client):
    """RewriteEngine raises ValueError for format-version outside {1, 2, 3}."""
    setup_bucket(s3_client)
    metadata = {
        "format-version": 4,
        "location": f"s3a://minio-bucket/{TABLE_PREFIX}",
    }
    s3_client.put_object(
        Bucket=BUCKET,
        Key=f"{TABLE_PREFIX}/metadata/v1.metadata.json",
        Body=json.dumps(metadata).encode(),
    )
    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)
    engine = RewriteEngine(CONFIG)
    with pytest.raises(ValueError, match="Unsupported Iceberg format-version"):
        engine.rewrite(graph, s3_client, BUCKET, TABLE_PREFIX)


def test_rewrite_engine_preserves_avro_codec(s3_client):
    """RewriteEngine writes rewritten Avro files with the same codec as the original."""
    setup_bucket(s3_client)
    build_simple_table(s3_client)
    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)
    # Original fixtures write with default null codec
    assert graph.manifest_lists[0].codec == "null"
    engine = RewriteEngine(CONFIG)
    result = engine.rewrite(graph, s3_client, BUCKET, TABLE_PREFIX)
    for avro_bytes in result.manifest_list_bytes.values():
        _, _, codec = load_avro_with_schema(avro_bytes)
        assert codec == "null"
    for avro_bytes in result.manifest_bytes.values():
        _, _, codec = load_avro_with_schema(avro_bytes)
        assert codec == "null"
