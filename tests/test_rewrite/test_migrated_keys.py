"""Tests for _migrated/ key remapping in RewriteEngine."""
import io
import json

import fastavro

from iceberg_migrate.rewrite.config import RewriteConfig
from iceberg_migrate.rewrite.engine import RewriteEngine, remap_key_to_migrated

BUCKET = "test-bucket"
TABLE_PREFIX = "warehouse/db/table"


def test_remap_key_to_migrated_basic():
    """remap_key_to_migrated inserts _migrated/ after table root."""
    key = "warehouse/db/table/metadata/v1.metadata.json"
    result = remap_key_to_migrated(key, TABLE_PREFIX)
    assert result == "warehouse/db/table/_migrated/metadata/v1.metadata.json"


def test_remap_key_to_migrated_manifest():
    """remap_key_to_migrated works for manifest avro keys."""
    key = "warehouse/db/table/metadata/abc-m0.avro"
    result = remap_key_to_migrated(key, TABLE_PREFIX)
    assert result == "warehouse/db/table/_migrated/metadata/abc-m0.avro"


def test_remap_key_to_migrated_preserves_subpath():
    """remap_key_to_migrated preserves everything after table root."""
    key = "warehouse/db/table/metadata/nested/deep/file.avro"
    result = remap_key_to_migrated(key, TABLE_PREFIX)
    assert result == "warehouse/db/table/_migrated/metadata/nested/deep/file.avro"


def test_rewrite_engine_uses_migrated_keys(s3_client):
    """RewriteEngine.rewrite remaps all output keys to _migrated/ paths."""
    s3_client.create_bucket(Bucket=BUCKET)

    ml_key = f"{TABLE_PREFIX}/metadata/snap-1.avro"
    manifest_key = f"{TABLE_PREFIX}/metadata/manifest-1.avro"

    manifest_schema = {
        "type": "record", "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int"},
            {"name": "data_file", "type": {
                "type": "record", "name": "data_file",
                "fields": [
                    {"name": "file_path", "type": "string"},
                    {"name": "file_size_in_bytes", "type": "long"},
                ],
            }},
        ],
    }
    manifest_records = [{"status": 1, "data_file": {
        "file_path": "s3a://old-bucket/warehouse/db/table/data/part-0.parquet",
        "file_size_in_bytes": 1024,
    }}]
    buf = io.BytesIO()
    fastavro.writer(buf, manifest_schema, manifest_records)
    s3_client.put_object(Bucket=BUCKET, Key=manifest_key, Body=buf.getvalue())

    ml_schema = {
        "type": "record", "name": "manifest_file",
        "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "manifest_length", "type": "long"},
        ],
    }
    ml_records = [{"manifest_path": f"s3://{BUCKET}/{manifest_key}", "manifest_length": 1000}]
    buf = io.BytesIO()
    fastavro.writer(buf, ml_schema, ml_records)
    s3_client.put_object(Bucket=BUCKET, Key=ml_key, Body=buf.getvalue())

    metadata = {
        "format-version": 2,
        "location": "s3a://old-bucket/warehouse/db/table",
        "current-snapshot-id": 1,
        "snapshots": [{"snapshot-id": 1, "manifest-list": f"s3://{BUCKET}/{ml_key}"}],
    }
    s3_client.put_object(
        Bucket=BUCKET,
        Key=f"{TABLE_PREFIX}/metadata/v1.metadata.json",
        Body=json.dumps(metadata).encode(),
    )

    from iceberg_migrate.discovery.reader import load_metadata_graph
    graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)
    config = RewriteConfig(src_prefix="s3a://old-bucket", dst_prefix="s3://new-bucket")
    engine = RewriteEngine(config)
    result = engine.rewrite(graph, s3_client, BUCKET, TABLE_PREFIX)

    # All keys should be under _migrated/
    assert "_migrated/" in result.graph.metadata_s3_key
    for key in result.manifest_list_bytes:
        assert "_migrated/" in key
    for key in result.manifest_bytes:
        assert "_migrated/" in key
