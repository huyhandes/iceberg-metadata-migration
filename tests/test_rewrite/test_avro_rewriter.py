"""Tests for rewrite_manifest_list_records and rewrite_manifest_records."""

import copy


from iceberg_migrate.rewrite.config import RewriteConfig
from iceberg_migrate.rewrite.avro_rewriter import (
    rewrite_manifest_list_records,
    rewrite_manifest_records,
)


CONFIG = RewriteConfig(
    src_prefix="s3a://src-bucket/warehouse",
    dst_prefix="s3://dst-bucket/warehouse",
)


# ---------------------------------------------------------------------------
# Test 7: rewrite_manifest_list_records rewrites manifest_path in all records (PATH-04)
# ---------------------------------------------------------------------------
def test_rewrite_manifest_list_records_all_paths():
    """rewrite_manifest_list_records rewrites manifest_path in all records."""
    records = [
        {
            "manifest_path": "s3a://src-bucket/warehouse/db/table/metadata/manifest-1.avro",
            "manifest_length": 1000,
        },
        {
            "manifest_path": "s3a://src-bucket/warehouse/db/table/metadata/manifest-2.avro",
            "manifest_length": 2000,
        },
    ]
    result = rewrite_manifest_list_records(records, CONFIG)
    assert result[0]["manifest_path"] == (
        "s3://dst-bucket/warehouse/db/table/metadata/manifest-1.avro"
    )
    assert result[1]["manifest_path"] == (
        "s3://dst-bucket/warehouse/db/table/metadata/manifest-2.avro"
    )


# ---------------------------------------------------------------------------
# Test 8: rewrite_manifest_records rewrites data_file.file_path in all records (PATH-05)
# ---------------------------------------------------------------------------
def test_rewrite_manifest_records_all_file_paths():
    """rewrite_manifest_records rewrites data_file.file_path in all records."""
    records = [
        {
            "status": 1,
            "data_file": {
                "file_path": "s3a://src-bucket/warehouse/db/table/data/part-0.parquet",
                "file_size_in_bytes": 1024,
            },
        },
        {
            "status": 1,
            "data_file": {
                "file_path": "s3a://src-bucket/warehouse/db/table/data/part-1.parquet",
                "file_size_in_bytes": 2048,
            },
        },
    ]
    result = rewrite_manifest_records(records, CONFIG)
    assert result[0]["data_file"]["file_path"] == (
        "s3://dst-bucket/warehouse/db/table/data/part-0.parquet"
    )
    assert result[1]["data_file"]["file_path"] == (
        "s3://dst-bucket/warehouse/db/table/data/part-1.parquet"
    )


# ---------------------------------------------------------------------------
# Test 9: rewrite_manifest_list_records returns deep copies
# ---------------------------------------------------------------------------
def test_rewrite_manifest_list_records_deep_copy():
    """rewrite_manifest_list_records returns deep copies — originals unmodified."""
    original_path = "s3a://src-bucket/warehouse/db/table/metadata/manifest-1.avro"
    records = [{"manifest_path": original_path, "manifest_length": 1000}]
    original_copy = copy.deepcopy(records)

    result = rewrite_manifest_list_records(records, CONFIG)

    assert result[0]["manifest_path"] != original_path
    # Original is unmodified
    assert records == original_copy


# ---------------------------------------------------------------------------
# Test 10: rewrite_manifest_records returns deep copies
# ---------------------------------------------------------------------------
def test_rewrite_manifest_records_deep_copy():
    """rewrite_manifest_records returns deep copies — originals unmodified."""
    original_path = "s3a://src-bucket/warehouse/db/table/data/part-0.parquet"
    records = [
        {
            "status": 1,
            "data_file": {"file_path": original_path, "file_size_in_bytes": 1024},
        }
    ]
    original_copy = copy.deepcopy(records)

    result = rewrite_manifest_records(records, CONFIG)

    assert result[0]["data_file"]["file_path"] != original_path
    # Original is unmodified
    assert records == original_copy


# ---------------------------------------------------------------------------
# Test 11: Paths not matching src_prefix are left unchanged
# ---------------------------------------------------------------------------
def test_paths_not_matching_src_prefix_unchanged():
    """Paths not matching src_prefix are left unchanged in both rewriters."""
    non_matching_path = "s3://other-bucket/some/path/manifest.avro"
    ml_records = [{"manifest_path": non_matching_path, "manifest_length": 500}]
    result = rewrite_manifest_list_records(ml_records, CONFIG)
    assert result[0]["manifest_path"] == non_matching_path

    non_matching_data_path = "s3://other-bucket/data/part-0.parquet"
    m_records = [
        {
            "status": 1,
            "data_file": {
                "file_path": non_matching_data_path,
                "file_size_in_bytes": 100,
            },
        }
    ]
    result_m = rewrite_manifest_records(m_records, CONFIG)
    assert result_m[0]["data_file"]["file_path"] == non_matching_data_path
