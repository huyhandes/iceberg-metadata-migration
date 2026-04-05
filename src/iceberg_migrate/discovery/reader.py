"""Metadata graph reader: load the full Iceberg metadata tree from S3.

Traverses metadata.json -> manifest list Avro -> manifest Avro files and
builds an in-memory IcebergMetadataGraph.

Key design decisions:
  - fastavro writer_schema is captured BEFORE iterating records (per D-08).
    Iterating to the end causes fastavro to discard the schema; capture it first.
  - s3a:// URIs in manifest paths are resolved by extracting the filename and
    fetching from the known table_prefix + "/metadata/" directory.  This avoids
    stale cross-account or cross-scheme URI references.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any, cast

import fastavro
import orjson

from iceberg_migrate.discovery.compression import decompress_metadata
from iceberg_migrate.discovery.locator import find_latest_metadata
from iceberg_migrate.models import IcebergMetadataGraph, ManifestFile, ManifestListFile
from iceberg_migrate.s3 import get_s3_object_bytes

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def load_avro_with_schema(
    data: bytes,
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    """Read Avro bytes and return the writer schema, all records, and the codec.

    The writer_schema is captured *before* iterating to ensure fastavro does
    not discard it mid-stream.

    Args:
        data: Raw Avro file bytes.

    Returns:
        A (writer_schema, records, codec) tuple where writer_schema is a dict,
        records is a list of dicts, and codec is the Avro container codec string.

    Raises:
        ValueError: If the Avro file has no embedded writer schema (invalid for Iceberg).
    """
    buf = io.BytesIO(data)
    reader = fastavro.reader(buf)
    # Capture writer_schema BEFORE consuming the iterator
    raw_schema = reader.writer_schema
    if raw_schema is None:
        raise ValueError(
            "Avro file has no embedded writer schema — invalid for Iceberg metadata"
        )
    writer_schema = cast(dict[str, Any], raw_schema)
    records = cast(list[dict[str, Any]], list(reader))
    # Capture codec from Avro container header
    codec_raw: Any = reader.metadata.get("avro.codec", "null")
    codec: str = (
        codec_raw.decode("utf-8") if isinstance(codec_raw, bytes) else str(codec_raw)
    )
    return writer_schema, records, codec


def resolve_avro_key(uri: str, table_prefix: str) -> str:
    """Resolve an Avro file URI to an S3 key under the known table prefix.

    Extracts the bare filename from the URI (ignoring scheme, bucket, and
    directory path) then constructs a key under ``<table_prefix>/metadata/``.

    This handles the common case where manifest URIs still reference the old
    s3a:// MinIO path after a DataSync copy.

    Args:
        uri: Any URI pointing to an Avro file (s3://, s3a://, or plain path).
        table_prefix: The target table prefix (no trailing slash).

    Returns:
        S3 key string: ``<table_prefix>/metadata/<filename>``.
    """
    filename = uri.split("/")[-1]
    return table_prefix.rstrip("/") + "/metadata/" + filename


def load_metadata_graph(
    s3_client: S3Client, bucket: str, table_prefix: str
) -> IcebergMetadataGraph:
    """Load the full Iceberg metadata graph from S3 into typed structures.

    Steps:
      1. Locate the latest metadata.json via ``find_latest_metadata``.
      2. Parse the JSON and find the current snapshot.
      3. Resolve + load the manifest list Avro referenced by the snapshot.
      4. For each entry in the manifest list, resolve + load the manifest Avro.
      5. Return a populated ``IcebergMetadataGraph``.

    If there is no ``current-snapshot-id`` in the metadata (e.g. a freshly
    created or snapshotless table), ``manifest_lists`` and ``manifests`` are
    left empty.

    Args:
        s3_client: A boto3 S3 client (real or moto-backed).
        bucket: S3 bucket name containing the table data.
        table_prefix: S3 prefix for the table root (no trailing slash).

    Returns:
        An ``IcebergMetadataGraph`` populated from the latest snapshot.
    """
    # Step 1: locate latest metadata.json
    metadata_key = find_latest_metadata(s3_client, bucket, table_prefix)
    metadata_bytes = get_s3_object_bytes(s3_client, bucket, metadata_key)
    # Some Iceberg writers (e.g. Lakekeeper) gzip-compress metadata files and name
    # them *.gz.metadata.json — decompress before JSON parsing.
    metadata_bytes = decompress_metadata(metadata_bytes, metadata_key)
    metadata_dict: dict[str, Any] = orjson.loads(metadata_bytes)

    graph = IcebergMetadataGraph(
        metadata_s3_key=metadata_key,
        metadata=metadata_dict,
    )

    # Step 2: find the current snapshot
    current_snapshot_id = metadata_dict.get("current-snapshot-id")
    if current_snapshot_id is None:
        return graph

    snapshots = metadata_dict.get("snapshots", [])
    current_snapshot = next(
        (s for s in snapshots if s.get("snapshot-id") == current_snapshot_id),
        None,
    )
    if current_snapshot is None:
        return graph

    # Step 3: load manifest list
    manifest_list_uri = current_snapshot.get("manifest-list", "")
    if not manifest_list_uri:
        return graph

    manifest_list_key = resolve_avro_key(manifest_list_uri, table_prefix)
    manifest_list_bytes = get_s3_object_bytes(s3_client, bucket, manifest_list_key)
    ml_schema, ml_records, ml_codec = load_avro_with_schema(manifest_list_bytes)

    manifest_list_file = ManifestListFile(
        s3_key=manifest_list_key,
        avro_schema=ml_schema,
        records=ml_records,
        codec=ml_codec,
    )
    graph.manifest_lists.append(manifest_list_file)

    # Step 4: load each individual manifest
    for record in ml_records:
        manifest_path = record.get("manifest_path", "")
        if not manifest_path:
            continue

        manifest_key = resolve_avro_key(manifest_path, table_prefix)
        manifest_bytes = get_s3_object_bytes(s3_client, bucket, manifest_key)
        m_schema, m_records, m_codec = load_avro_with_schema(manifest_bytes)

        manifest_file = ManifestFile(
            s3_key=manifest_key,
            avro_schema=m_schema,
            records=m_records,
            codec=m_codec,
        )
        graph.manifests.append(manifest_file)

    return graph
