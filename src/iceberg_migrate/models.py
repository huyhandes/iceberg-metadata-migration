"""Typed data structures for the in-memory Iceberg metadata graph."""
from typing import Any

from pydantic import BaseModel


class ManifestListFile(BaseModel):
    """One manifest list Avro file loaded from S3.

    Contains entries that each reference a manifest file via manifest_path.
    """
    s3_key: str
    avro_schema: dict[str, Any]     # writer_schema preserved for Phase 2 round-trip writes
    records: list[dict[str, Any]]   # Each record has 'manifest_path' field


class ManifestFile(BaseModel):
    """One manifest Avro file loaded from S3.

    Contains entries that each reference data files via data_file.file_path.
    """
    s3_key: str
    avro_schema: dict[str, Any]     # writer_schema preserved for Phase 2 round-trip writes
    records: list[dict[str, Any]]   # Each record has 'data_file' dict with 'file_path'


class IcebergMetadataGraph(BaseModel):
    """Full in-memory representation of a loaded Iceberg table's metadata.

    The metadata dict is the raw parsed metadata.json.
    manifest_lists and manifests are loaded from the Avro files referenced
    in the current snapshot.
    """
    metadata_s3_key: str
    metadata: dict[str, Any]        # Raw parsed metadata.json
    manifest_lists: list[ManifestListFile] = []
    manifests: list[ManifestFile] = []
