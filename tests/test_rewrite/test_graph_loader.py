"""Tests for collect_all_manifest_list_uris and load_full_graph."""

import io
import json

import fastavro

from iceberg_migrate.discovery.reader import load_metadata_graph
from iceberg_migrate.rewrite.graph_loader import (
    collect_all_manifest_list_uris,
    load_full_graph,
)


BUCKET = "test-bucket"
TABLE_PREFIX = "warehouse/db/table"


# ---------------------------------------------------------------------------
# Avro fixture helpers (same as test_reader.py)
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


def setup_bucket(s3_client) -> None:
    s3_client.create_bucket(Bucket=BUCKET)


# ---------------------------------------------------------------------------
# Test 1: collect_all_manifest_list_uris returns URIs from ALL snapshots
# ---------------------------------------------------------------------------
def test_collect_all_manifest_list_uris_all_snapshots():
    """collect_all_manifest_list_uris returns manifest-list URIs from ALL snapshots."""
    metadata = {
        "format-version": 2,
        "snapshots": [
            {"snapshot-id": 1, "manifest-list": "s3://bucket/metadata/snap-1.avro"},
            {"snapshot-id": 2, "manifest-list": "s3://bucket/metadata/snap-2.avro"},
            {"snapshot-id": 3, "manifest-list": "s3://bucket/metadata/snap-3.avro"},
        ],
    }
    uris = collect_all_manifest_list_uris(metadata)
    assert uris == [
        "s3://bucket/metadata/snap-1.avro",
        "s3://bucket/metadata/snap-2.avro",
        "s3://bucket/metadata/snap-3.avro",
    ]


# ---------------------------------------------------------------------------
# Test 2: collect_all_manifest_list_uris returns empty list when no snapshots
# ---------------------------------------------------------------------------
def test_collect_all_manifest_list_uris_no_snapshots():
    """collect_all_manifest_list_uris returns empty list when metadata has no snapshots."""
    metadata = {"format-version": 2, "location": "s3://bucket/table"}
    uris = collect_all_manifest_list_uris(metadata)
    assert uris == []


# ---------------------------------------------------------------------------
# Test 3: collect_all_manifest_list_uris deduplicates URIs
# ---------------------------------------------------------------------------
def test_collect_all_manifest_list_uris_deduplication():
    """collect_all_manifest_list_uris deduplicates URIs from snapshots sharing a manifest list."""
    shared_uri = "s3://bucket/metadata/snap-1.avro"
    metadata = {
        "format-version": 2,
        "snapshots": [
            {"snapshot-id": 1, "manifest-list": shared_uri},
            {"snapshot-id": 2, "manifest-list": shared_uri},  # Same as snapshot 1
            {"snapshot-id": 3, "manifest-list": "s3://bucket/metadata/snap-3.avro"},
        ],
    }
    uris = collect_all_manifest_list_uris(metadata)
    assert uris == [shared_uri, "s3://bucket/metadata/snap-3.avro"]
    assert len(uris) == 2


# ---------------------------------------------------------------------------
# Test 4: load_full_graph loads manifest lists from ALL snapshots
# ---------------------------------------------------------------------------
def test_load_full_graph_loads_all_snapshots(s3_client):
    """load_full_graph extends graph with manifest lists from ALL snapshots."""
    setup_bucket(s3_client)

    ml_key_1 = f"{TABLE_PREFIX}/metadata/snap-1-manifest-list.avro"
    ml_key_2 = f"{TABLE_PREFIX}/metadata/snap-2-manifest-list.avro"
    manifest_key_1 = f"{TABLE_PREFIX}/metadata/manifest-1.avro"
    manifest_key_2 = f"{TABLE_PREFIX}/metadata/manifest-2.avro"

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
    upload_bytes(
        s3_client,
        manifest_key_1,
        make_manifest_avro([f"s3://{BUCKET}/data/part-0.parquet"]),
    )
    upload_bytes(
        s3_client,
        manifest_key_2,
        make_manifest_avro([f"s3://{BUCKET}/data/part-1.parquet"]),
    )

    metadata = {
        "format-version": 2,
        "location": f"s3://{BUCKET}/{TABLE_PREFIX}",
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

    # Phase 1 loads only current snapshot (snapshot 2)
    phase1_graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)
    assert len(phase1_graph.manifest_lists) == 1
    assert phase1_graph.manifest_lists[0].s3_key == ml_key_2

    # load_full_graph should also load snapshot 1's manifest list
    full_graph = load_full_graph(phase1_graph, s3_client, BUCKET, TABLE_PREFIX)

    ml_keys = {ml.s3_key for ml in full_graph.manifest_lists}
    assert ml_key_1 in ml_keys
    assert ml_key_2 in ml_keys
    assert len(full_graph.manifest_lists) == 2
    assert len(full_graph.manifests) == 2


# ---------------------------------------------------------------------------
# Test 5: load_full_graph does not duplicate manifest lists already in graph
# ---------------------------------------------------------------------------
def test_load_full_graph_no_duplicate_manifest_lists(s3_client):
    """load_full_graph does not duplicate manifest lists already present in graph."""
    setup_bucket(s3_client)

    ml_key = f"{TABLE_PREFIX}/metadata/snap-1-manifest-list.avro"
    manifest_key = f"{TABLE_PREFIX}/metadata/manifest-1.avro"

    upload_bytes(
        s3_client, ml_key, make_manifest_list_avro([f"s3://{BUCKET}/{manifest_key}"])
    )
    upload_bytes(
        s3_client,
        manifest_key,
        make_manifest_avro([f"s3://{BUCKET}/data/part-0.parquet"]),
    )

    metadata = {
        "format-version": 2,
        "location": f"s3://{BUCKET}/{TABLE_PREFIX}",
        "current-snapshot-id": 1,
        "snapshots": [
            {
                "snapshot-id": 1,
                "manifest-list": f"s3://{BUCKET}/{ml_key}",
            },
        ],
    }
    upload_bytes(
        s3_client,
        f"{TABLE_PREFIX}/metadata/v1.metadata.json",
        json.dumps(metadata).encode(),
    )

    # Phase 1 loads the only snapshot
    phase1_graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)
    assert len(phase1_graph.manifest_lists) == 1

    # load_full_graph should not duplicate the already-loaded manifest list
    full_graph = load_full_graph(phase1_graph, s3_client, BUCKET, TABLE_PREFIX)
    assert len(full_graph.manifest_lists) == 1
    assert len(full_graph.manifests) == 1


# ---------------------------------------------------------------------------
# Test 6: load_full_graph does not duplicate manifests shared across snapshots
# ---------------------------------------------------------------------------
def test_load_full_graph_no_duplicate_manifests(s3_client):
    """load_full_graph does not duplicate manifests shared across multiple snapshots."""
    setup_bucket(s3_client)

    ml_key_1 = f"{TABLE_PREFIX}/metadata/snap-1-manifest-list.avro"
    ml_key_2 = f"{TABLE_PREFIX}/metadata/snap-2-manifest-list.avro"
    # Both manifest lists reference the same manifest file
    shared_manifest_key = f"{TABLE_PREFIX}/metadata/manifest-shared.avro"

    upload_bytes(
        s3_client,
        ml_key_1,
        make_manifest_list_avro([f"s3://{BUCKET}/{shared_manifest_key}"]),
    )
    upload_bytes(
        s3_client,
        ml_key_2,
        make_manifest_list_avro([f"s3://{BUCKET}/{shared_manifest_key}"]),
    )
    upload_bytes(
        s3_client,
        shared_manifest_key,
        make_manifest_avro([f"s3://{BUCKET}/data/part-0.parquet"]),
    )

    metadata = {
        "format-version": 2,
        "location": f"s3://{BUCKET}/{TABLE_PREFIX}",
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

    phase1_graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)
    full_graph = load_full_graph(phase1_graph, s3_client, BUCKET, TABLE_PREFIX)

    # Two distinct manifest lists, but only one manifest (shared across both)
    assert len(full_graph.manifest_lists) == 2
    assert len(full_graph.manifests) == 1
    assert full_graph.manifests[0].s3_key == shared_manifest_key


# ---------------------------------------------------------------------------
# Test 7: load_full_graph handles table with no snapshots gracefully
# ---------------------------------------------------------------------------
def test_load_full_graph_no_snapshots(s3_client):
    """load_full_graph handles table with no snapshots — returns graph unchanged."""
    setup_bucket(s3_client)

    metadata = {
        "format-version": 2,
        "location": f"s3://{BUCKET}/{TABLE_PREFIX}",
    }
    upload_bytes(
        s3_client,
        f"{TABLE_PREFIX}/metadata/v1.metadata.json",
        json.dumps(metadata).encode(),
    )

    phase1_graph = load_metadata_graph(s3_client, BUCKET, TABLE_PREFIX)
    assert len(phase1_graph.manifest_lists) == 0

    full_graph = load_full_graph(phase1_graph, s3_client, BUCKET, TABLE_PREFIX)
    assert len(full_graph.manifest_lists) == 0
    assert len(full_graph.manifests) == 0
    assert full_graph.metadata_s3_key == phase1_graph.metadata_s3_key
