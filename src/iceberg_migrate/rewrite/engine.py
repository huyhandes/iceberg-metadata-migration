"""RewriteEngine: orchestrates path rewriting across all Iceberg metadata layers.

Automatically loads ALL manifest lists from ALL snapshots via load_full_graph
before rewriting, ensuring time-travel safety per D-06, D-08, D-09, D-11.

Output is a RewriteResult containing:
  - Rewritten IcebergMetadataGraph (in-memory)
  - metadata_bytes: orjson-serialized rewritten metadata.json
  - manifest_list_bytes: fastavro-serialized rewritten manifest list Avro files
  - manifest_bytes: fastavro-serialized rewritten manifest Avro files
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any

import fastavro
import orjson

from iceberg_migrate.models import IcebergMetadataGraph, ManifestListFile, ManifestFile
from iceberg_migrate.rewrite.config import RewriteConfig

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
from iceberg_migrate.rewrite.graph_loader import load_full_graph
from iceberg_migrate.rewrite.metadata_rewriter import rewrite_metadata_json
from iceberg_migrate.rewrite.avro_rewriter import (
    rewrite_manifest_list_records,
    rewrite_manifest_records,
)


def _remap_snapshot_manifest_lists(metadata: dict[str, Any]) -> dict[str, Any]:
    """Remap manifest-list paths in snapshots from metadata/ to _migrated/metadata/.

    After src→dst prefix rewrite, manifest-list paths point to
    <table_root>/metadata/<filename>.  The actual rewritten Avro files are
    written to <table_root>/_migrated/metadata/<filename>.  This function
    updates the references so Athena follows the rewritten (migrated) copies.
    """
    import copy

    m = copy.deepcopy(metadata)
    dst_table_root = m.get("location", "").rstrip("/")
    if not dst_table_root:
        return m
    old_prefix = dst_table_root + "/metadata/"
    new_prefix = dst_table_root + "/_migrated/metadata/"
    for snap in m.get("snapshots", []):
        if "manifest-list" in snap:
            path = snap["manifest-list"]
            if path.startswith(old_prefix):
                snap["manifest-list"] = new_prefix + path[len(old_prefix):]
    return m


def _remap_manifest_paths(
    records: list[dict[str, Any]], dst_table_root: str
) -> list[dict[str, Any]]:
    """Remap manifest_path entries from metadata/ to _migrated/metadata/.

    After src→dst prefix rewrite, manifest_path values point to
    <table_root>/metadata/<filename>.  The actual rewritten Avro files are
    written to <table_root>/_migrated/metadata/<filename>.
    """
    import copy

    old_prefix = dst_table_root.rstrip("/") + "/metadata/"
    new_prefix = dst_table_root.rstrip("/") + "/_migrated/metadata/"
    result = copy.deepcopy(records)
    for record in result:
        path = record.get("manifest_path", "")
        if path and path.startswith(old_prefix):
            record["manifest_path"] = new_prefix + path[len(old_prefix):]
    return result


class RewriteResult:
    """Container for rewrite output including serialized bytes.

    Uses a plain __init__ (not Pydantic) because bytes fields are not JSON-serializable.
    """

    graph: IcebergMetadataGraph
    metadata_bytes: bytes
    manifest_list_bytes: dict[str, bytes]  # s3_key -> rewritten Avro bytes
    manifest_bytes: dict[str, bytes]  # s3_key -> rewritten Avro bytes

    def __init__(
        self,
        graph: IcebergMetadataGraph,
        metadata_bytes: bytes,
        manifest_list_bytes: dict[str, bytes],  # s3_key -> rewritten Avro bytes
        manifest_bytes: dict[str, bytes],  # s3_key -> rewritten Avro bytes
    ):
        self.graph = graph
        self.metadata_bytes = metadata_bytes
        self.manifest_list_bytes = manifest_list_bytes
        self.manifest_bytes = manifest_bytes


def remap_key_to_migrated(key: str, table_prefix: str) -> str:
    """Remap an S3 key to the _migrated/ subdirectory under the table root.

    Given table_prefix='warehouse/db/table' and key='warehouse/db/table/metadata/v1.metadata.json',
    returns 'warehouse/db/table/_migrated/metadata/v1.metadata.json'.

    Gzip-compressed metadata files (*.gz.metadata.json) are renamed to
    *.metadata.json because the _migrated/ copy is written as plain JSON.
    Athena uses the file extension to determine decoding, so removing .gz
    prevents it from attempting to gzip-decompress plain JSON content.
    """
    prefix = table_prefix.rstrip("/")
    suffix = key[len(prefix) + 1 :] if key.startswith(prefix + "/") else key
    # Strip .gz compression marker from metadata filenames
    if suffix.endswith(".gz.metadata.json"):
        suffix = suffix[: -len(".gz.metadata.json")] + ".metadata.json"
    return f"{prefix}/_migrated/{suffix}"


class RewriteEngine:
    """Orchestrates path rewriting across all metadata layers.

    Automatically loads ALL manifest lists from ALL snapshots via
    load_full_graph before rewriting, ensuring time-travel safety
    per D-06, D-08, D-09, D-11.
    """

    config: RewriteConfig

    def __init__(self, config: RewriteConfig):
        self.config = config

    def rewrite(
        self,
        graph: IcebergMetadataGraph,
        s3_client: S3Client,
        bucket: str,
        table_prefix: str,
    ) -> RewriteResult:
        """Rewrite all paths in the metadata graph.

        First calls load_full_graph to ensure ALL snapshots' manifest
        lists and manifests are loaded (Phase 1 reader only loads
        current snapshot). Then rewrites all path-bearing fields and
        serializes results to bytes.

        Args:
            graph: IcebergMetadataGraph from Phase 1 reader (may only
                   have current snapshot's files loaded).
            s3_client: boto3 S3 client for loading missing files.
            bucket: S3 bucket name.
            table_prefix: S3 prefix for the table root.

        Returns:
            RewriteResult with fully rewritten graph and serialized bytes.
        """
        # Step 0: Ensure ALL snapshots' files are loaded
        full_graph = load_full_graph(graph, s3_client, bucket, table_prefix)

        # Step 1: Rewrite metadata.json dict
        rewritten_metadata = rewrite_metadata_json(full_graph.metadata, self.config)
        # Remap manifest-list references from metadata/ → _migrated/metadata/ so
        # downstream consumers (Athena) follow the rewritten Avro copies, not the
        # originals that still contain stale MinIO paths.
        rewritten_metadata = _remap_snapshot_manifest_lists(rewritten_metadata)
        dst_table_root = rewritten_metadata.get("location", "").rstrip("/")

        # Step 2: Serialize rewritten metadata to bytes via orjson
        metadata_bytes = orjson.dumps(rewritten_metadata, option=orjson.OPT_INDENT_2)

        # Step 3: Rewrite all manifest list records and serialize to Avro
        rewritten_ml_list = []
        ml_bytes_map: dict[str, bytes] = {}
        for ml in full_graph.manifest_lists:
            new_records = rewrite_manifest_list_records(ml.records, self.config)
            # Remap manifest_path references from metadata/ → _migrated/metadata/
            new_records = _remap_manifest_paths(new_records, dst_table_root)
            new_ml = ManifestListFile(
                s3_key=ml.s3_key,
                avro_schema=ml.avro_schema,
                records=new_records,
            )
            rewritten_ml_list.append(new_ml)
            ml_bytes_map[ml.s3_key] = self._serialize_avro(ml.avro_schema, new_records)

        # Step 4: Rewrite all manifest records and serialize to Avro
        rewritten_m_list = []
        m_bytes_map: dict[str, bytes] = {}
        for m in full_graph.manifests:
            new_records = rewrite_manifest_records(m.records, self.config)
            new_m = ManifestFile(
                s3_key=m.s3_key,
                avro_schema=m.avro_schema,
                records=new_records,
            )
            rewritten_m_list.append(new_m)
            m_bytes_map[m.s3_key] = self._serialize_avro(m.avro_schema, new_records)

        # Step 6: Remap all keys to _migrated/ paths
        migrated_metadata_key = remap_key_to_migrated(
            full_graph.metadata_s3_key, table_prefix
        )

        migrated_ml_bytes: dict[str, bytes] = {}
        migrated_ml_list: list[ManifestListFile] = []
        for ml in rewritten_ml_list:
            new_key = remap_key_to_migrated(ml.s3_key, table_prefix)
            migrated_ml_bytes[new_key] = ml_bytes_map[ml.s3_key]
            migrated_ml_list.append(
                ManifestListFile(
                    s3_key=new_key,
                    avro_schema=ml.avro_schema,
                    records=ml.records,
                )
            )

        migrated_m_bytes: dict[str, bytes] = {}
        migrated_m_list: list[ManifestFile] = []
        for m in rewritten_m_list:
            new_key = remap_key_to_migrated(m.s3_key, table_prefix)
            migrated_m_bytes[new_key] = m_bytes_map[m.s3_key]
            migrated_m_list.append(
                ManifestFile(
                    s3_key=new_key,
                    avro_schema=m.avro_schema,
                    records=m.records,
                )
            )

        rewritten_graph = IcebergMetadataGraph(
            metadata_s3_key=migrated_metadata_key,
            metadata=rewritten_metadata,
            manifest_lists=migrated_ml_list,
            manifests=migrated_m_list,
        )

        return RewriteResult(
            graph=rewritten_graph,
            metadata_bytes=metadata_bytes,
            manifest_list_bytes=migrated_ml_bytes,
            manifest_bytes=migrated_m_bytes,
        )

    @staticmethod
    def _serialize_avro(schema: dict[str, Any], records: list[dict[str, Any]]) -> bytes:
        """Serialize records to Avro bytes using the given writer schema."""
        buf = io.BytesIO()
        fastavro.writer(buf, schema, records)
        return buf.getvalue()
