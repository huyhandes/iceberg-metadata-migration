"""Tests for v3 deletion vector path rewriting in rewrite_manifest_records.

Covers:
  - v3 manifest records with deletion_vector.path field
  - v3 manifest records with deletion_vector.file_location field (guard per Open Question 1)
  - v2 manifest records without deletion_vector (regression guard)
  - v3 manifest records with deletion_vector=None (null safety)
"""

from iceberg_migrate.rewrite.avro_rewriter import rewrite_manifest_records
from iceberg_migrate.rewrite.config import RewriteConfig

SRC = "s3a://minio-bucket/warehouse"
DST = "s3://aws-bucket/warehouse"
CONFIG = RewriteConfig(src_prefix=SRC, dst_prefix=DST)


def test_v3_deletion_vector_path_rewritten():
    """Test 1: v3 manifest record with data_file.deletion_vector.path gets path rewritten."""
    records = [
        {
            "data_file": {
                "file_path": f"{SRC}/db/table/data/0001.parquet",
                "deletion_vector": {
                    "path": f"{SRC}/db/table/dv/0001.bin",
                    "offset": 0,
                    "length": 100,
                },
            }
        }
    ]
    result = rewrite_manifest_records(records, CONFIG)
    data_file = result[0]["data_file"]
    assert data_file["file_path"] == f"{DST}/db/table/data/0001.parquet"
    dv = data_file["deletion_vector"]
    assert dv["path"] == f"{DST}/db/table/dv/0001.bin"
    # Other fields unchanged
    assert dv["offset"] == 0
    assert dv["length"] == 100


def test_v3_deletion_vector_file_location_rewritten():
    """Test 2: v3 manifest with deletion_vector.file_location (alternate field name) gets rewritten."""
    records = [
        {
            "data_file": {
                "file_path": f"{SRC}/db/table/data/0002.parquet",
                "deletion_vector": {
                    "file_location": f"{SRC}/db/table/dv/0002.bin",
                    "offset": 0,
                    "length": 200,
                },
            }
        }
    ]
    result = rewrite_manifest_records(records, CONFIG)
    data_file = result[0]["data_file"]
    assert data_file["file_path"] == f"{DST}/db/table/data/0002.parquet"
    dv = data_file["deletion_vector"]
    assert dv["file_location"] == f"{DST}/db/table/dv/0002.bin"


def test_v2_manifest_without_deletion_vector_unchanged():
    """Test 3: v2 manifest record WITHOUT deletion_vector is rewritten identically to Phase 2."""
    records = [
        {
            "data_file": {
                "file_path": f"{SRC}/db/table/data/0003.parquet",
            }
        }
    ]
    result = rewrite_manifest_records(records, CONFIG)
    data_file = result[0]["data_file"]
    assert data_file["file_path"] == f"{DST}/db/table/data/0003.parquet"
    # No deletion_vector key present
    assert "deletion_vector" not in data_file


def test_v3_deletion_vector_none_handled_gracefully():
    """Test 4: v3 manifest record with deletion_vector=None is handled gracefully (no error)."""
    records = [
        {
            "data_file": {
                "file_path": f"{SRC}/db/table/data/0004.parquet",
                "deletion_vector": None,
            }
        }
    ]
    result = rewrite_manifest_records(records, CONFIG)
    data_file = result[0]["data_file"]
    assert data_file["file_path"] == f"{DST}/db/table/data/0004.parquet"
    # deletion_vector stays None, no error raised
    assert data_file["deletion_vector"] is None
