"""All-snapshot graph loading for time-travel safe rewriting.

Phase 1's load_metadata_graph only loads the current snapshot's manifest
list and manifests. For time-travel safety (D-06 through D-09), the
rewrite engine needs ALL snapshots' manifest lists and manifests loaded.

This module extends an existing IcebergMetadataGraph by fetching any
manifest lists and manifests not yet loaded.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from iceberg_migrate.discovery.reader import load_avro_with_schema, resolve_avro_key
from iceberg_migrate.models import IcebergMetadataGraph, ManifestListFile, ManifestFile
from iceberg_migrate.s3 import get_s3_object_bytes

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def collect_all_manifest_list_uris(metadata: dict[str, Any]) -> list[str]:
    """Return deduplicated manifest-list URIs from every snapshot in metadata.json.

    Args:
        metadata: Parsed metadata.json dict.

    Returns:
        Deduplicated list of manifest-list URI strings (order preserved).
    """
    seen = set()
    uris = []
    for snap in metadata.get("snapshots", []):
        uri = snap.get("manifest-list")
        if uri and uri not in seen:
            seen.add(uri)
            uris.append(uri)
    return uris


def load_full_graph(
    graph: IcebergMetadataGraph,
    s3_client: S3Client,
    bucket: str,
    table_prefix: str,
) -> IcebergMetadataGraph:
    """Extend graph with manifest lists and manifests from ALL snapshots.

    Loads any manifest lists referenced by snapshots in metadata.json that
    are not already present in graph.manifest_lists. For each newly loaded
    manifest list, loads all referenced manifest files not already present
    in graph.manifests.

    Does NOT mutate the input graph — returns a new IcebergMetadataGraph.

    Args:
        graph: Existing graph (typically from Phase 1 load_metadata_graph,
               which only has the current snapshot's files).
        s3_client: boto3 S3 client.
        bucket: S3 bucket name.
        table_prefix: S3 prefix for the table root (no trailing slash).

    Returns:
        New IcebergMetadataGraph with all snapshots' files loaded.
    """
    all_uris = collect_all_manifest_list_uris(graph.metadata)

    # Track already-loaded keys to avoid duplicates
    loaded_ml_keys = {ml.s3_key for ml in graph.manifest_lists}
    loaded_m_keys = {m.s3_key for m in graph.manifests}

    new_manifest_lists = list(graph.manifest_lists)
    new_manifests = list(graph.manifests)

    for uri in all_uris:
        ml_key = resolve_avro_key(uri, table_prefix)
        if ml_key in loaded_ml_keys:
            # Already loaded (e.g., current snapshot's manifest list from Phase 1)
            # But still need to check its manifest records for new manifests
            existing_ml = next(ml for ml in new_manifest_lists if ml.s3_key == ml_key)
            _load_manifests_from_records(
                existing_ml.records,
                table_prefix,
                s3_client,
                bucket,
                loaded_m_keys,
                new_manifests,
            )
            continue

        # Load new manifest list
        ml_bytes = get_s3_object_bytes(s3_client, bucket, ml_key)
        ml_schema, ml_records = load_avro_with_schema(ml_bytes)
        ml_file = ManifestListFile(
            s3_key=ml_key,
            avro_schema=ml_schema,
            records=ml_records,
        )
        new_manifest_lists.append(ml_file)
        loaded_ml_keys.add(ml_key)

        # Load manifests referenced by this manifest list
        _load_manifests_from_records(
            ml_records,
            table_prefix,
            s3_client,
            bucket,
            loaded_m_keys,
            new_manifests,
        )

    return IcebergMetadataGraph(
        metadata_s3_key=graph.metadata_s3_key,
        metadata=graph.metadata,
        manifest_lists=new_manifest_lists,
        manifests=new_manifests,
    )


def _load_manifests_from_records(
    ml_records: list[dict[str, Any]],
    table_prefix: str,
    s3_client: S3Client,
    bucket: str,
    loaded_m_keys: set[str],
    manifests_out: list[ManifestFile],
) -> None:
    """Load manifest files referenced by manifest list records, skipping duplicates."""
    for record in ml_records:
        manifest_path = record.get("manifest_path", "")
        if not manifest_path:
            continue
        m_key = resolve_avro_key(manifest_path, table_prefix)
        if m_key in loaded_m_keys:
            continue
        m_bytes = get_s3_object_bytes(s3_client, bucket, m_key)
        m_schema, m_records = load_avro_with_schema(m_bytes)
        m_file = ManifestFile(
            s3_key=m_key,
            avro_schema=m_schema,
            records=m_records,
        )
        manifests_out.append(m_file)
        loaded_m_keys.add(m_key)
