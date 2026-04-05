"""End-to-end integration test: full Phase 2 pipeline.

Tests the complete pipeline:
  load_metadata_graph (Phase 1) -> RewriteEngine.rewrite -> validate_rewrite

Exercises:
  - Multi-snapshot table (2 snapshots with separate manifest lists)
  - metadata-log rewriting
  - load_full_graph integration (all snapshots loaded, not just current)
  - Byte-level validation: no s3a:// in rewritten files
  - Structural validation: required fields present
  - Manifest count validation: 4 before == 4 after
  - All paths rewritten from s3a://minio-bucket to s3://aws-bucket
"""

import io
import json

import fastavro
import pytest
from moto import mock_aws
import boto3

from iceberg_migrate.discovery.reader import load_metadata_graph
from iceberg_migrate.rewrite.config import RewriteConfig
from iceberg_migrate.rewrite.engine import RewriteEngine
from iceberg_migrate.rewrite.graph_loader import load_full_graph
from iceberg_migrate.validation.validator import validate_rewrite


BUCKET = "test-bucket"
TABLE_PREFIX = "warehouse/db/table"
SRC_PREFIX = "s3a://minio-bucket/warehouse"
DST_PREFIX = "s3://aws-bucket/warehouse"

CONFIG = RewriteConfig(
    src_prefix="s3a://minio-bucket",
    dst_prefix="s3://aws-bucket",
)


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


def upload_bytes(s3_client, key: str, data: bytes) -> None:
    s3_client.put_object(Bucket=BUCKET, Key=key, Body=data)


# ---------------------------------------------------------------------------
# End-to-end integration test
# ---------------------------------------------------------------------------


@pytest.fixture
def s3_client():
    """Moto-backed S3 client with a realistic multi-snapshot Iceberg table."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def setup_multi_snapshot_table(s3_client):
    """Upload a realistic Iceberg table to S3 with 2 snapshots.

    Structure:
      - metadata.json: 2 snapshots, 2 metadata-log entries, current-snapshot-id=2
      - snap-1.avro: manifest list for snapshot 1 (2 manifest paths)
      - snap-2.avro: manifest list for snapshot 2 (2 manifest paths)
      - manifest-1.avro: 2 data files (snapshot 1)
      - manifest-2.avro: 2 data files (snapshot 1)
      - manifest-3.avro: 2 data files (snapshot 2)
      - manifest-4.avro: 2 data files (snapshot 2)
    All paths use SRC_PREFIX = s3a://minio-bucket/warehouse
    """
    # Manifest list keys
    ml_key_1 = f"{TABLE_PREFIX}/metadata/snap-1.avro"
    ml_key_2 = f"{TABLE_PREFIX}/metadata/snap-2.avro"

    # Manifest keys (2 per snapshot)
    manifest_key_1 = f"{TABLE_PREFIX}/metadata/manifest-1.avro"
    manifest_key_2 = f"{TABLE_PREFIX}/metadata/manifest-2.avro"
    manifest_key_3 = f"{TABLE_PREFIX}/metadata/manifest-3.avro"
    manifest_key_4 = f"{TABLE_PREFIX}/metadata/manifest-4.avro"

    # Upload manifests (data file paths use SRC_PREFIX)
    upload_bytes(
        s3_client,
        manifest_key_1,
        make_manifest_avro(
            [
                f"{SRC_PREFIX}/db/table/data/snap1-part-0.parquet",
                f"{SRC_PREFIX}/db/table/data/snap1-part-1.parquet",
            ]
        ),
    )
    upload_bytes(
        s3_client,
        manifest_key_2,
        make_manifest_avro(
            [
                f"{SRC_PREFIX}/db/table/data/snap1-part-2.parquet",
                f"{SRC_PREFIX}/db/table/data/snap1-part-3.parquet",
            ]
        ),
    )
    upload_bytes(
        s3_client,
        manifest_key_3,
        make_manifest_avro(
            [
                f"{SRC_PREFIX}/db/table/data/snap2-part-0.parquet",
                f"{SRC_PREFIX}/db/table/data/snap2-part-1.parquet",
            ]
        ),
    )
    upload_bytes(
        s3_client,
        manifest_key_4,
        make_manifest_avro(
            [
                f"{SRC_PREFIX}/db/table/data/snap2-part-2.parquet",
                f"{SRC_PREFIX}/db/table/data/snap2-part-3.parquet",
            ]
        ),
    )

    # Upload manifest lists (manifest_path values use the target bucket s3:// URI)
    # The reader resolves s3:// URIs by filename extraction, so use actual bucket URIs
    upload_bytes(
        s3_client,
        ml_key_1,
        make_manifest_list_avro(
            [
                f"s3://{BUCKET}/{manifest_key_1}",
                f"s3://{BUCKET}/{manifest_key_2}",
            ]
        ),
    )
    upload_bytes(
        s3_client,
        ml_key_2,
        make_manifest_list_avro(
            [
                f"s3://{BUCKET}/{manifest_key_3}",
                f"s3://{BUCKET}/{manifest_key_4}",
            ]
        ),
    )

    # Upload metadata.json
    # - 2 snapshots: snap-1 (historical) and snap-2 (current)
    # - 2 metadata-log entries
    # - manifest-list URIs use actual bucket s3:// (reader resolves by filename)
    metadata = {
        "format-version": 2,
        "table-uuid": "integration-test-uuid",
        "location": f"{SRC_PREFIX}/db/table",
        "current-snapshot-id": 2,
        "snapshots": [
            {
                "snapshot-id": 1,
                "timestamp-ms": 1700000000000,
                "manifest-list": f"s3://{BUCKET}/{ml_key_1}",
                "summary": {"operation": "append"},
            },
            {
                "snapshot-id": 2,
                "timestamp-ms": 1700000001000,
                "manifest-list": f"s3://{BUCKET}/{ml_key_2}",
                "summary": {"operation": "append"},
            },
        ],
        "metadata-log": [
            {
                "timestamp-ms": 1699999000000,
                "metadata-file": f"{SRC_PREFIX}/db/table/metadata/v1.metadata.json",
            },
            {
                "timestamp-ms": 1700000000000,
                "metadata-file": f"{SRC_PREFIX}/db/table/metadata/v2.metadata.json",
            },
        ],
    }
    upload_bytes(
        s3_client,
        f"{TABLE_PREFIX}/metadata/v2.metadata.json",
        json.dumps(metadata).encode(),
    )
    return metadata, ml_key_1, ml_key_2


def test_full_pipeline_rewrite_and_validate(s3_client):
    """End-to-end: Phase 1 load -> Phase 2 rewrite -> validate — all checks pass.

    Verifies:
      - Phase 1 graph only loads current snapshot (snapshot 2)
      - RewriteEngine calls load_full_graph so ALL snapshots are rewritten
      - validate_rewrite passes all three checks
      - No s3a:// remains in any rewritten file
      - metadata-log entries are rewritten
      - result.graph has 2 manifest lists (proves all-snapshot loading)
      - manifest count matches: 4 before, 4 after
    """
    setup_multi_snapshot_table(s3_client)

    # Step 1: Phase 1 load — only loads current snapshot (snapshot 2)
    phase1_graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)

    # Phase 1 only loads snapshot 2's manifest list and its manifests
    assert len(phase1_graph.manifest_lists) == 1, "Phase 1 only loads current snapshot"
    assert len(phase1_graph.manifests) == 2, (
        "Phase 1 only loads current snapshot's manifests"
    )

    # Step 2: Load full graph for count comparison (all snapshots)
    full_original_graph = load_full_graph(phase1_graph, s3_client, BUCKET, TABLE_PREFIX)
    assert len(full_original_graph.manifest_lists) == 2, (
        "Full graph has 2 manifest lists"
    )
    assert len(full_original_graph.manifests) == 4, "Full graph has 4 manifests"

    # Step 3: Rewrite via engine (internally calls load_full_graph)
    engine = RewriteEngine(CONFIG)
    result = engine.rewrite(phase1_graph, s3_client, BUCKET, TABLE_PREFIX)

    # Rewritten graph must have ALL snapshots
    assert len(result.graph.manifest_lists) == 2, (
        "Rewritten graph has 2 manifest lists (all snapshots)"
    )
    assert len(result.graph.manifests) == 4, "Rewritten graph has 4 manifests"

    # Step 4: Validate using full original graph for count comparison
    validation = validate_rewrite(full_original_graph, result, SRC_PREFIX)

    # ----- Core assertions -----
    assert validation.passed is True, (
        f"Validation should pass. Errors: {validation.errors}"
    )
    assert validation.residual_prefix_count == 0, (
        "No residual source prefix in any rewritten file"
    )
    assert validation.structural_valid is True, (
        "Rewritten metadata.json must be structurally valid"
    )
    assert validation.errors == [], (
        f"No validation errors expected, got: {validation.errors}"
    )

    # ----- Manifest count assertions -----
    assert validation.manifest_count_before == 4, "4 manifests before rewrite"
    assert validation.manifest_count_after == 4, "4 manifests after rewrite"
    assert validation.manifest_list_count_before == 2, "2 manifest lists before rewrite"
    assert validation.manifest_list_count_after == 2, "2 manifest lists after rewrite"

    # ----- Rewritten metadata assertions -----
    import orjson

    rewritten_metadata = orjson.loads(result.metadata_bytes)

    # location is rewritten
    assert rewritten_metadata["location"].startswith("s3://aws-bucket"), (
        f"location should start with s3://aws-bucket, got: {rewritten_metadata['location']}"
    )

    # Both snapshots' manifest-list paths are rewritten
    for snap in rewritten_metadata["snapshots"]:
        ml_path = snap["manifest-list"]
        assert "s3://aws-bucket" in ml_path or f"s3://{BUCKET}" in ml_path, (
            f"Snapshot manifest-list should be rewritten: {ml_path}"
        )
        assert "s3a://minio-bucket" not in ml_path, (
            f"s3a:// should be gone from snapshot manifest-list: {ml_path}"
        )

    # metadata-log entries are rewritten
    for entry in rewritten_metadata.get("metadata-log", []):
        mf = entry.get("metadata-file", "")
        assert "s3a://minio-bucket" not in mf, (
            f"s3a:// should be gone from metadata-log entry: {mf}"
        )
        assert mf.startswith("s3://aws-bucket"), (
            f"metadata-log entry should start with s3://aws-bucket: {mf}"
        )

    # ----- Rewritten Avro assertions -----
    from iceberg_migrate.discovery.reader import load_avro_with_schema

    # All manifest list Avro bytes: manifest_path values rewritten
    for key, ml_bytes in result.manifest_list_bytes.items():
        _, records, _ = load_avro_with_schema(ml_bytes)
        for rec in records:
            mp = rec.get("manifest_path", "")
            assert "s3a://" not in mp, (
                f"s3a:// should be gone from manifest_path in {key}: {mp}"
            )

    # All manifest Avro bytes: data_file.file_path values rewritten
    for key, m_bytes in result.manifest_bytes.items():
        _, records, _ = load_avro_with_schema(m_bytes)
        for rec in records:
            fp = rec.get("data_file", {}).get("file_path", "")
            assert "s3a://" not in fp, (
                f"s3a:// should be gone from file_path in {key}: {fp}"
            )
            assert fp.startswith("s3://aws-bucket"), (
                f"file_path should start with s3://aws-bucket in {key}: {fp}"
            )

    # ----- No s3a:// anywhere in rewritten files -----
    assert b"s3a://" not in result.metadata_bytes, (
        "metadata_bytes must not contain s3a://"
    )
    for key, bytes_val in result.manifest_list_bytes.items():
        assert b"s3a://" not in bytes_val, (
            f"manifest_list_bytes[{key}] must not contain s3a://"
        )
    for key, bytes_val in result.manifest_bytes.items():
        assert b"s3a://" not in bytes_val, (
            f"manifest_bytes[{key}] must not contain s3a://"
        )
