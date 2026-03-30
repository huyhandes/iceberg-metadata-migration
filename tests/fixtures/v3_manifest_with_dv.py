"""Fixture helpers for building synthetic Iceberg metadata in S3 for integration tests.

Provides create_test_metadata() to seed S3 with:
  - metadata.json (format-version 2 or 3)
  - manifest list Avro (snap-*.avro)
  - manifest Avro (manifest-*.avro)

All paths use src_prefix. If format_version=3, manifest records include
deletion_vector.path field.

Returns the metadata.json S3 key for reference.
"""
from __future__ import annotations

import io
import json

import fastavro


# ---------------------------------------------------------------------------
# Avro schemas
# ---------------------------------------------------------------------------

_MANIFEST_LIST_SCHEMA = {
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string"},
        {"name": "manifest_length", "type": "long"},
        {"name": "partition_spec_id", "type": "int"},
        {"name": "added_snapshot_id", "type": ["null", "long"], "default": None},
    ],
}

_MANIFEST_SCHEMA_V2 = {
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
                    {"name": "file_format", "type": "string"},
                    {"name": "file_size_in_bytes", "type": "long"},
                    {"name": "record_count", "type": "long"},
                ],
            },
        },
    ],
}

_MANIFEST_SCHEMA_V3 = {
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
                    {"name": "file_format", "type": "string"},
                    {"name": "file_size_in_bytes", "type": "long"},
                    {"name": "record_count", "type": "long"},
                    {
                        "name": "deletion_vector",
                        "type": [
                            "null",
                            {
                                "type": "record",
                                "name": "deletion_vector",
                                "fields": [
                                    {"name": "path", "type": "string"},
                                    {"name": "offset", "type": "long"},
                                    {"name": "length", "type": "int"},
                                ],
                            },
                        ],
                        "default": None,
                    },
                ],
            },
        },
    ],
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_test_metadata(
    s3_client,
    bucket: str,
    table_prefix: str,
    src_prefix: str,
    format_version: int = 2,
) -> str:
    """Seed S3 with synthetic Iceberg metadata files for integration testing.

    Seeds three files:
      - {table_prefix}/metadata/00001-abc.metadata.json
      - {table_prefix}/metadata/snap-123.avro  (manifest list)
      - {table_prefix}/metadata/manifest-001.avro  (manifest)

    All embedded paths use src_prefix. If format_version=3, manifest records
    include a deletion_vector.path field.

    Args:
        s3_client: A boto3 S3 client (real or moto-mocked).
        bucket: Target S3 bucket name.
        table_prefix: S3 key prefix for the table root (no leading slash, no trailing slash).
        src_prefix: Source prefix to embed in all paths (e.g., "s3a://minio-bucket/warehouse").
        format_version: Iceberg format version (2 or 3). Default: 2.

    Returns:
        S3 key of the metadata.json file (for use as table_location reference).
    """
    table_prefix = table_prefix.rstrip("/")
    src_prefix = src_prefix.rstrip("/")

    metadata_key = f"{table_prefix}/metadata/00001-abc.metadata.json"
    manifest_list_key = f"{table_prefix}/metadata/snap-123.avro"
    manifest_key = f"{table_prefix}/metadata/manifest-001.avro"

    # Full S3 URIs as used inside the metadata files
    manifest_list_uri = f"{src_prefix}/{table_prefix}/metadata/snap-123.avro"
    manifest_uri = f"{src_prefix}/{table_prefix}/metadata/manifest-001.avro"
    data_file_uri = f"{src_prefix}/{table_prefix}/data/part-00000.parquet"
    dv_path_uri = f"{src_prefix}/{table_prefix}/data/delete-00000.dv"
    table_location_uri = f"{src_prefix}/{table_prefix}"

    # ----- Build metadata.json -----
    metadata = {
        "format-version": format_version,
        "table-uuid": "test-uuid-integration",
        "location": table_location_uri,
        "current-snapshot-id": 123,
        "snapshots": [
            {
                "snapshot-id": 123,
                "timestamp-ms": 1700000000000,
                "manifest-list": manifest_list_uri,
                "summary": {"operation": "append"},
            }
        ],
        "metadata-log": [
            {
                "timestamp-ms": 1700000000000,
                "metadata-file": f"{src_prefix}/{metadata_key}",
            }
        ],
    }
    metadata_bytes = json.dumps(metadata).encode("utf-8")

    # ----- Build manifest list Avro -----
    ml_records = [
        {
            "manifest_path": manifest_uri,
            "manifest_length": 512,
            "partition_spec_id": 0,
            "added_snapshot_id": 123,
        }
    ]
    manifest_list_bytes = _write_avro(_MANIFEST_LIST_SCHEMA, ml_records)

    # ----- Build manifest Avro -----
    if format_version == 3:
        schema = _MANIFEST_SCHEMA_V3
        manifest_records = [
            {
                "status": 1,
                "data_file": {
                    "file_path": data_file_uri,
                    "file_format": "PARQUET",
                    "file_size_in_bytes": 1024,
                    "record_count": 100,
                    "deletion_vector": {
                        "path": dv_path_uri,
                        "offset": 0,
                        "length": 64,
                    },
                },
            }
        ]
    else:
        schema = _MANIFEST_SCHEMA_V2
        manifest_records = [
            {
                "status": 1,
                "data_file": {
                    "file_path": data_file_uri,
                    "file_format": "PARQUET",
                    "file_size_in_bytes": 1024,
                    "record_count": 100,
                },
            }
        ]
    manifest_bytes = _write_avro(schema, manifest_records)

    # ----- Upload to S3 -----
    s3_client.put_object(Bucket=bucket, Key=metadata_key, Body=metadata_bytes)
    s3_client.put_object(Bucket=bucket, Key=manifest_list_key, Body=manifest_list_bytes)
    s3_client.put_object(Bucket=bucket, Key=manifest_key, Body=manifest_bytes)

    return metadata_key


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _write_avro(schema: dict, records: list[dict]) -> bytes:
    """Serialize records to Avro bytes using the given schema."""
    buf = io.BytesIO()
    fastavro.writer(buf, schema, records)
    return buf.getvalue()
