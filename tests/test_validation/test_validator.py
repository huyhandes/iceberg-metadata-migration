"""Tests for ValidationResult model and validate_rewrite function.

Covers:
  VAL-01: Byte-level scan — zero source prefix occurrences in all rewritten bytes
  VAL-02: Structural validation — metadata.json parseable with required fields
  VAL-03: Manifest count preservation — counts match before and after rewrite
  D-15:   All three checks must pass for passed=True
"""
import io

import fastavro
import orjson
import pytest

from iceberg_migrate.models import IcebergMetadataGraph, ManifestListFile, ManifestFile
from iceberg_migrate.rewrite.engine import RewriteResult
from iceberg_migrate.validation.validator import validate_rewrite, ValidationResult


# ---------------------------------------------------------------------------
# Helpers to build minimal Avro bytes
# ---------------------------------------------------------------------------

def _make_manifest_list_avro(manifest_paths: list[str]) -> bytes:
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


def _make_manifest_avro(file_paths: list[str]) -> bytes:
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


_ML_SCHEMA = {
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string"},
        {"name": "manifest_length", "type": "long"},
    ],
}
_M_SCHEMA = {
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

SRC_PREFIX = "s3a://minio-bucket/warehouse"
DST_PREFIX = "s3://aws-bucket/warehouse"


def _build_passing_result():
    """Build a RewriteResult and original_graph that should pass all validations."""
    # Rewritten metadata.json bytes — uses DST_PREFIX, no SRC_PREFIX
    metadata = {
        "format-version": 2,
        "location": f"{DST_PREFIX}/db/table",
        "current-snapshot-id": 1,
        "snapshots": [
            {
                "snapshot-id": 1,
                "manifest-list": f"{DST_PREFIX}/db/table/metadata/snap-1.avro",
            }
        ],
    }
    metadata_bytes = orjson.dumps(metadata)

    # Rewritten manifest list Avro bytes — uses DST_PREFIX
    ml_path = f"{DST_PREFIX}/db/table/metadata/manifest-1.avro"
    ml_bytes = _make_manifest_list_avro([ml_path])
    manifest_list_bytes = {"warehouse/db/table/metadata/snap-1.avro": ml_bytes}

    # Rewritten manifest Avro bytes — uses DST_PREFIX
    data_path = f"{DST_PREFIX}/db/table/data/part-0.parquet"
    m_bytes = _make_manifest_avro([data_path])
    manifest_bytes = {"warehouse/db/table/metadata/manifest-1.avro": m_bytes}

    # Build the rewritten graph (1 manifest list, 1 manifest)
    rewritten_graph = IcebergMetadataGraph(
        metadata_s3_key="warehouse/db/table/metadata/v1.metadata.json",
        metadata=metadata,
        manifest_lists=[
            ManifestListFile(
                s3_key="warehouse/db/table/metadata/snap-1.avro",
                avro_schema=_ML_SCHEMA,
                records=[{"manifest_path": ml_path, "manifest_length": 1000}],
            )
        ],
        manifests=[
            ManifestFile(
                s3_key="warehouse/db/table/metadata/manifest-1.avro",
                avro_schema=_M_SCHEMA,
                records=[{"status": 1, "data_file": {"file_path": data_path, "file_size_in_bytes": 1024}}],
            )
        ],
    )

    result = RewriteResult(
        graph=rewritten_graph,
        metadata_bytes=metadata_bytes,
        manifest_list_bytes=manifest_list_bytes,
        manifest_bytes=manifest_bytes,
    )

    # Original graph (same counts, uses SRC_PREFIX)
    orig_ml_path = f"{SRC_PREFIX}/db/table/metadata/manifest-1.avro"
    orig_data_path = f"{SRC_PREFIX}/db/table/data/part-0.parquet"
    original_graph = IcebergMetadataGraph(
        metadata_s3_key="warehouse/db/table/metadata/v1.metadata.json",
        metadata={
            "format-version": 2,
            "location": f"{SRC_PREFIX}/db/table",
            "current-snapshot-id": 1,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "manifest-list": f"{SRC_PREFIX}/db/table/metadata/snap-1.avro",
                }
            ],
        },
        manifest_lists=[
            ManifestListFile(
                s3_key="warehouse/db/table/metadata/snap-1.avro",
                avro_schema=_ML_SCHEMA,
                records=[{"manifest_path": orig_ml_path, "manifest_length": 1000}],
            )
        ],
        manifests=[
            ManifestFile(
                s3_key="warehouse/db/table/metadata/manifest-1.avro",
                avro_schema=_M_SCHEMA,
                records=[{"status": 1, "data_file": {"file_path": orig_data_path, "file_size_in_bytes": 1024}}],
            )
        ],
    )

    return original_graph, result


# ===========================================================================
# VAL-01: Byte-level scan tests
# ===========================================================================

# Test 1: Passing case — no source prefix in any rewritten bytes
def test_val01_no_residual_prefix_passes():
    """validate_rewrite returns passed=True when no source prefix found in any rewritten bytes."""
    original_graph, result = _build_passing_result()
    validation = validate_rewrite(original_graph, result, SRC_PREFIX)
    assert validation.passed is True
    assert validation.residual_prefix_count == 0
    assert validation.errors == []


# Test 2: Source prefix still in metadata_bytes
def test_val01_residual_in_metadata_bytes_fails():
    """validate_rewrite returns passed=False when source prefix found in metadata_bytes."""
    original_graph, result = _build_passing_result()
    # Inject source prefix into metadata_bytes
    bad_metadata = {
        "format-version": 2,
        "location": f"{SRC_PREFIX}/db/table",  # Not rewritten!
        "current-snapshot-id": 1,
        "snapshots": [{"snapshot-id": 1, "manifest-list": f"{DST_PREFIX}/db/table/metadata/snap-1.avro"}],
    }
    bad_result = RewriteResult(
        graph=result.graph,
        metadata_bytes=orjson.dumps(bad_metadata),
        manifest_list_bytes=result.manifest_list_bytes,
        manifest_bytes=result.manifest_bytes,
    )
    validation = validate_rewrite(original_graph, bad_result, SRC_PREFIX)
    assert validation.passed is False
    assert validation.residual_prefix_count > 0
    assert len(validation.errors) > 0


# Test 3: Source prefix still in manifest list Avro bytes
def test_val01_residual_in_manifest_list_avro_bytes_fails():
    """validate_rewrite catches source prefix in manifest list Avro bytes."""
    original_graph, result = _build_passing_result()
    # Inject source prefix into manifest list Avro bytes
    bad_ml_bytes = _make_manifest_list_avro([f"{SRC_PREFIX}/db/table/metadata/manifest-1.avro"])
    bad_result = RewriteResult(
        graph=result.graph,
        metadata_bytes=result.metadata_bytes,
        manifest_list_bytes={"warehouse/db/table/metadata/snap-1.avro": bad_ml_bytes},
        manifest_bytes=result.manifest_bytes,
    )
    validation = validate_rewrite(original_graph, bad_result, SRC_PREFIX)
    assert validation.passed is False
    assert validation.residual_prefix_count > 0


# Test 4: Source prefix still in manifest Avro bytes
def test_val01_residual_in_manifest_avro_bytes_fails():
    """validate_rewrite catches source prefix in manifest Avro bytes."""
    original_graph, result = _build_passing_result()
    # Inject source prefix into manifest Avro bytes
    bad_m_bytes = _make_manifest_avro([f"{SRC_PREFIX}/db/table/data/part-0.parquet"])
    bad_result = RewriteResult(
        graph=result.graph,
        metadata_bytes=result.metadata_bytes,
        manifest_list_bytes=result.manifest_list_bytes,
        manifest_bytes={"warehouse/db/table/metadata/manifest-1.avro": bad_m_bytes},
    )
    validation = validate_rewrite(original_graph, bad_result, SRC_PREFIX)
    assert validation.passed is False
    assert validation.residual_prefix_count > 0


# ===========================================================================
# VAL-02: Structural validation tests
# ===========================================================================

# Test 5: Structural check passes with valid JSON + required fields
def test_val02_structural_passes_with_valid_metadata():
    """validate_rewrite returns passed=True when metadata_bytes is valid JSON with required fields."""
    original_graph, result = _build_passing_result()
    validation = validate_rewrite(original_graph, result, SRC_PREFIX)
    assert validation.structural_valid is True


# Test 6: Structural check fails with invalid JSON
def test_val02_structural_fails_with_invalid_json():
    """validate_rewrite returns structural_valid=False when metadata_bytes is not valid JSON."""
    original_graph, result = _build_passing_result()
    bad_result = RewriteResult(
        graph=result.graph,
        metadata_bytes=b"NOT VALID JSON {{{",
        manifest_list_bytes=result.manifest_list_bytes,
        manifest_bytes=result.manifest_bytes,
    )
    validation = validate_rewrite(original_graph, bad_result, SRC_PREFIX)
    assert validation.structural_valid is False
    assert validation.passed is False
    assert any("JSON" in e or "json" in e.lower() for e in validation.errors)


# Test 7: Structural check fails when "location" field is missing
def test_val02_structural_fails_missing_location():
    """validate_rewrite returns structural_valid=False when required field 'location' is missing."""
    original_graph, result = _build_passing_result()
    # metadata without 'location'
    bad_metadata = {
        "format-version": 2,
        "current-snapshot-id": 1,
        "snapshots": [{"snapshot-id": 1, "manifest-list": f"{DST_PREFIX}/snap-1.avro"}],
    }
    bad_result = RewriteResult(
        graph=result.graph,
        metadata_bytes=orjson.dumps(bad_metadata),
        manifest_list_bytes=result.manifest_list_bytes,
        manifest_bytes=result.manifest_bytes,
    )
    validation = validate_rewrite(original_graph, bad_result, SRC_PREFIX)
    assert validation.structural_valid is False
    assert validation.passed is False
    assert any("location" in e for e in validation.errors)


# Test 8: Structural check fails when "snapshots" field is missing
def test_val02_structural_fails_missing_snapshots():
    """validate_rewrite returns structural_valid=False when required field 'snapshots' is missing."""
    original_graph, result = _build_passing_result()
    bad_metadata = {
        "format-version": 2,
        "location": f"{DST_PREFIX}/db/table",
        "current-snapshot-id": 1,
        # "snapshots" intentionally omitted
    }
    bad_result = RewriteResult(
        graph=result.graph,
        metadata_bytes=orjson.dumps(bad_metadata),
        manifest_list_bytes=result.manifest_list_bytes,
        manifest_bytes=result.manifest_bytes,
    )
    validation = validate_rewrite(original_graph, bad_result, SRC_PREFIX)
    assert validation.structural_valid is False
    assert validation.passed is False
    assert any("snapshots" in e for e in validation.errors)


# ===========================================================================
# VAL-03: Manifest count tests
# ===========================================================================

# Test 9: Manifest count matches (3 before, 3 after)
def test_val03_manifest_count_matches_passes():
    """validate_rewrite returns passed=True when manifest count matches."""
    # Build a graph with 3 manifests
    manifests = [
        ManifestFile(
            s3_key=f"warehouse/db/table/metadata/manifest-{i}.avro",
            avro_schema=_M_SCHEMA,
            records=[{"status": 1, "data_file": {"file_path": f"{DST_PREFIX}/data/part-{i}.parquet", "file_size_in_bytes": 1024}}],
        )
        for i in range(3)
    ]
    manifest_lists = [
        ManifestListFile(
            s3_key="warehouse/db/table/metadata/snap-1.avro",
            avro_schema=_ML_SCHEMA,
            records=[{"manifest_path": f"{DST_PREFIX}/manifest-{i}.avro", "manifest_length": 1000} for i in range(3)],
        )
    ]
    metadata = {
        "format-version": 2,
        "location": f"{DST_PREFIX}/db/table",
        "current-snapshot-id": 1,
        "snapshots": [{"snapshot-id": 1, "manifest-list": f"{DST_PREFIX}/snap-1.avro"}],
    }
    rewritten_graph = IcebergMetadataGraph(
        metadata_s3_key="warehouse/db/table/metadata/v1.metadata.json",
        metadata=metadata,
        manifest_lists=manifest_lists,
        manifests=manifests,
    )
    # Build bytes — no src prefix
    ml_bytes = _make_manifest_list_avro([f"{DST_PREFIX}/manifest-{i}.avro" for i in range(3)])
    m_bytes_map = {
        f"warehouse/db/table/metadata/manifest-{i}.avro": _make_manifest_avro([f"{DST_PREFIX}/data/part-{i}.parquet"])
        for i in range(3)
    }
    result = RewriteResult(
        graph=rewritten_graph,
        metadata_bytes=orjson.dumps(metadata),
        manifest_list_bytes={"warehouse/db/table/metadata/snap-1.avro": ml_bytes},
        manifest_bytes=m_bytes_map,
    )

    # Original graph also has 3 manifests
    orig_manifests = [
        ManifestFile(
            s3_key=f"warehouse/db/table/metadata/manifest-{i}.avro",
            avro_schema=_M_SCHEMA,
            records=[],
        )
        for i in range(3)
    ]
    original_graph = IcebergMetadataGraph(
        metadata_s3_key="warehouse/db/table/metadata/v1.metadata.json",
        metadata={"format-version": 2, "location": f"{SRC_PREFIX}/db/table", "current-snapshot-id": 1, "snapshots": []},
        manifest_lists=manifest_lists,
        manifests=orig_manifests,
    )

    validation = validate_rewrite(original_graph, result, SRC_PREFIX)
    assert validation.passed is True
    assert validation.manifest_count_before == 3
    assert validation.manifest_count_after == 3


# Test 10: Manifest count differs (3 before, 2 after)
def test_val03_manifest_count_mismatch_fails():
    """validate_rewrite returns passed=False when manifest count differs."""
    original_graph, result = _build_passing_result()

    # Original has 1 manifest, but we'll create an original with 2 manifests
    extra_manifest = ManifestFile(
        s3_key="warehouse/db/table/metadata/manifest-extra.avro",
        avro_schema=_M_SCHEMA,
        records=[],
    )
    original_graph_extended = IcebergMetadataGraph(
        metadata_s3_key=original_graph.metadata_s3_key,
        metadata=original_graph.metadata,
        manifest_lists=original_graph.manifest_lists,
        manifests=original_graph.manifests + [extra_manifest],  # 2 manifests in original
    )

    # result.graph still has 1 manifest (count mismatch: 2 before, 1 after)
    validation = validate_rewrite(original_graph_extended, result, SRC_PREFIX)
    assert validation.passed is False
    assert validation.manifest_count_before == 2
    assert validation.manifest_count_after == 1
    assert any("manifest count" in e.lower() or "mismatch" in e.lower() for e in validation.errors)


# Test 11: Manifest list count differs
def test_val03_manifest_list_count_mismatch_fails():
    """validate_rewrite returns passed=False when manifest_list count differs."""
    original_graph, result = _build_passing_result()

    # Add extra manifest list to original (2 original, 1 after)
    extra_ml = ManifestListFile(
        s3_key="warehouse/db/table/metadata/snap-extra.avro",
        avro_schema=_ML_SCHEMA,
        records=[],
    )
    original_graph_extended = IcebergMetadataGraph(
        metadata_s3_key=original_graph.metadata_s3_key,
        metadata=original_graph.metadata,
        manifest_lists=original_graph.manifest_lists + [extra_ml],
        manifests=original_graph.manifests,
    )

    validation = validate_rewrite(original_graph_extended, result, SRC_PREFIX)
    assert validation.passed is False
    assert validation.manifest_list_count_before == 2
    assert validation.manifest_list_count_after == 1


# ===========================================================================
# Integration: D-15 — all three checks must pass
# ===========================================================================

# Test 12: All three checks pass
def test_d15_all_checks_pass():
    """validate_rewrite returns passed=True only when ALL three checks pass."""
    original_graph, result = _build_passing_result()
    validation = validate_rewrite(original_graph, result, SRC_PREFIX)
    assert validation.passed is True
    assert validation.structural_valid is True
    assert validation.residual_prefix_count == 0
    assert validation.manifest_count_before == validation.manifest_count_after
    assert validation.errors == []


# Test 13: ValidationResult.errors contains human-readable descriptions
def test_d15_errors_are_human_readable():
    """ValidationResult.errors contains human-readable descriptions of all failures."""
    original_graph, result = _build_passing_result()

    # Create a result that fails all three checks:
    # VAL-01: src prefix still in metadata_bytes
    # VAL-02: not valid JSON
    # VAL-03: manifest count mismatch
    bad_bytes = SRC_PREFIX.encode() + b" some content"  # Not valid JSON, contains src prefix

    extra_manifest = ManifestFile(
        s3_key="warehouse/db/table/metadata/manifest-extra.avro",
        avro_schema=_M_SCHEMA,
        records=[],
    )
    original_graph_extended = IcebergMetadataGraph(
        metadata_s3_key=original_graph.metadata_s3_key,
        metadata=original_graph.metadata,
        manifest_lists=original_graph.manifest_lists,
        manifests=original_graph.manifests + [extra_manifest],
    )

    bad_result = RewriteResult(
        graph=result.graph,
        metadata_bytes=bad_bytes,
        manifest_list_bytes=result.manifest_list_bytes,
        manifest_bytes=result.manifest_bytes,
    )

    validation = validate_rewrite(original_graph_extended, bad_result, SRC_PREFIX)
    assert validation.passed is False
    assert len(validation.errors) >= 2  # At least VAL-01 + VAL-03 errors
    # Errors must be non-empty strings
    for err in validation.errors:
        assert isinstance(err, str)
        assert len(err) > 0
