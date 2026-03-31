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
from iceberg_migrate.rewrite.avro_rewriter import rewrite_manifest_list_records, rewrite_manifest_records


class RewriteResult:
    """Container for rewrite output including serialized bytes.

    Uses a plain __init__ (not Pydantic) because bytes fields are not JSON-serializable.
    """
    graph: IcebergMetadataGraph
    metadata_bytes: bytes
    manifest_list_bytes: dict[str, bytes]  # s3_key -> rewritten Avro bytes
    manifest_bytes: dict[str, bytes]        # s3_key -> rewritten Avro bytes

    def __init__(
        self,
        graph: IcebergMetadataGraph,
        metadata_bytes: bytes,
        manifest_list_bytes: dict[str, bytes],   # s3_key -> rewritten Avro bytes
        manifest_bytes: dict[str, bytes],         # s3_key -> rewritten Avro bytes
    ):
        self.graph = graph
        self.metadata_bytes = metadata_bytes
        self.manifest_list_bytes = manifest_list_bytes
        self.manifest_bytes = manifest_bytes


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

        # Step 2: Serialize rewritten metadata to bytes via orjson
        metadata_bytes = orjson.dumps(rewritten_metadata, option=orjson.OPT_INDENT_2)

        # Step 3: Rewrite all manifest list records and serialize to Avro
        rewritten_ml_list = []
        ml_bytes_map: dict[str, bytes] = {}
        for ml in full_graph.manifest_lists:
            new_records = rewrite_manifest_list_records(ml.records, self.config)
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

        # Step 5: Build rewritten graph
        rewritten_graph = IcebergMetadataGraph(
            metadata_s3_key=full_graph.metadata_s3_key,
            metadata=rewritten_metadata,
            manifest_lists=rewritten_ml_list,
            manifests=rewritten_m_list,
        )

        return RewriteResult(
            graph=rewritten_graph,
            metadata_bytes=metadata_bytes,
            manifest_list_bytes=ml_bytes_map,
            manifest_bytes=m_bytes_map,
        )

    @staticmethod
    def _serialize_avro(schema: dict[str, Any], records: list[dict[str, Any]]) -> bytes:
        """Serialize records to Avro bytes using the given writer schema."""
        buf = io.BytesIO()
        fastavro.writer(buf, schema, records)
        return buf.getvalue()
