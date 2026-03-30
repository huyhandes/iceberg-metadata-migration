"""Tests for rewrite_metadata_json — rewrites all path-bearing fields in metadata.json."""
import copy

import pytest

from iceberg_migrate.rewrite.config import RewriteConfig
from iceberg_migrate.rewrite.metadata_rewriter import rewrite_metadata_json


CONFIG = RewriteConfig(
    src_prefix="s3a://src-bucket/warehouse",
    dst_prefix="s3://dst-bucket/warehouse",
)


# ---------------------------------------------------------------------------
# Test 1: location field rewritten (PATH-01)
# ---------------------------------------------------------------------------
def test_rewrite_metadata_json_location():
    """rewrite_metadata_json rewrites metadata['location'] from src to dst prefix."""
    metadata = {
        "format-version": 2,
        "location": "s3a://src-bucket/warehouse/db/table",
        "snapshots": [],
    }
    result = rewrite_metadata_json(metadata, CONFIG)
    assert result["location"] == "s3://dst-bucket/warehouse/db/table"


# ---------------------------------------------------------------------------
# Test 2: ALL snapshots' manifest-list paths rewritten (PATH-02)
# ---------------------------------------------------------------------------
def test_rewrite_metadata_json_all_snapshot_manifest_lists():
    """rewrite_metadata_json rewrites ALL snapshots' manifest-list URIs, not just current."""
    metadata = {
        "format-version": 2,
        "location": "s3a://src-bucket/warehouse/db/table",
        "current-snapshot-id": 2,
        "snapshots": [
            {
                "snapshot-id": 1,
                "manifest-list": "s3a://src-bucket/warehouse/db/table/metadata/snap-1.avro",
            },
            {
                "snapshot-id": 2,
                "manifest-list": "s3a://src-bucket/warehouse/db/table/metadata/snap-2.avro",
            },
        ],
    }
    result = rewrite_metadata_json(metadata, CONFIG)
    assert result["snapshots"][0]["manifest-list"] == (
        "s3://dst-bucket/warehouse/db/table/metadata/snap-1.avro"
    )
    assert result["snapshots"][1]["manifest-list"] == (
        "s3://dst-bucket/warehouse/db/table/metadata/snap-2.avro"
    )


# ---------------------------------------------------------------------------
# Test 3: ALL metadata-log entries rewritten (PATH-03)
# ---------------------------------------------------------------------------
def test_rewrite_metadata_json_all_metadata_log_entries():
    """rewrite_metadata_json rewrites ALL metadata-log entries."""
    metadata = {
        "format-version": 2,
        "location": "s3a://src-bucket/warehouse/db/table",
        "metadata-log": [
            {"metadata-file": "s3a://src-bucket/warehouse/db/table/metadata/v1.metadata.json"},
            {"metadata-file": "s3a://src-bucket/warehouse/db/table/metadata/v2.metadata.json"},
        ],
    }
    result = rewrite_metadata_json(metadata, CONFIG)
    assert result["metadata-log"][0]["metadata-file"] == (
        "s3://dst-bucket/warehouse/db/table/metadata/v1.metadata.json"
    )
    assert result["metadata-log"][1]["metadata-file"] == (
        "s3://dst-bucket/warehouse/db/table/metadata/v2.metadata.json"
    )


# ---------------------------------------------------------------------------
# Test 4: Original metadata dict is not modified (deep copy)
# ---------------------------------------------------------------------------
def test_rewrite_metadata_json_returns_deep_copy():
    """rewrite_metadata_json returns a deep copy — original metadata is unmodified."""
    original_location = "s3a://src-bucket/warehouse/db/table"
    metadata = {
        "format-version": 2,
        "location": original_location,
        "snapshots": [
            {
                "snapshot-id": 1,
                "manifest-list": "s3a://src-bucket/warehouse/db/table/metadata/snap-1.avro",
            }
        ],
    }
    original_copy = copy.deepcopy(metadata)
    result = rewrite_metadata_json(metadata, CONFIG)

    # Result is rewritten
    assert result["location"] != original_location
    # Original is unchanged
    assert metadata["location"] == original_location
    assert metadata == original_copy


# ---------------------------------------------------------------------------
# Test 5: Missing optional arrays handled gracefully
# ---------------------------------------------------------------------------
def test_rewrite_metadata_json_missing_optional_arrays():
    """rewrite_metadata_json handles metadata without metadata-log or statistics."""
    metadata = {
        "format-version": 2,
        "location": "s3a://src-bucket/warehouse/db/table",
    }
    result = rewrite_metadata_json(metadata, CONFIG)
    assert result["location"] == "s3://dst-bucket/warehouse/db/table"
    # No exception raised for missing optional fields


# ---------------------------------------------------------------------------
# Test 6: statistics and partition-statistics paths rewritten
# ---------------------------------------------------------------------------
def test_rewrite_metadata_json_statistics_paths():
    """rewrite_metadata_json rewrites statistics[].statistics-path and partition-statistics[].statistics-path."""
    metadata = {
        "format-version": 2,
        "location": "s3a://src-bucket/warehouse/db/table",
        "statistics": [
            {
                "snapshot-id": 1,
                "statistics-path": "s3a://src-bucket/warehouse/db/table/metadata/stats-1.puffin",
            }
        ],
        "partition-statistics": [
            {
                "snapshot-id": 1,
                "statistics-path": "s3a://src-bucket/warehouse/db/table/metadata/pstats-1.puffin",
            }
        ],
    }
    result = rewrite_metadata_json(metadata, CONFIG)
    assert result["statistics"][0]["statistics-path"] == (
        "s3://dst-bucket/warehouse/db/table/metadata/stats-1.puffin"
    )
    assert result["partition-statistics"][0]["statistics-path"] == (
        "s3://dst-bucket/warehouse/db/table/metadata/pstats-1.puffin"
    )
