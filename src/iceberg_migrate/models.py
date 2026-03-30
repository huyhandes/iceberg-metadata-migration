"""Typed data structures for the in-memory Iceberg metadata graph."""
from dataclasses import dataclass, field


@dataclass
class ManifestListFile:
    """One manifest list Avro file loaded from S3.

    Contains entries that each reference a manifest file via manifest_path.
    """
    s3_key: str
    avro_schema: dict          # writer_schema preserved for Phase 2 round-trip writes
    records: list[dict]        # Each record has 'manifest_path' field


@dataclass
class ManifestFile:
    """One manifest Avro file loaded from S3.

    Contains entries that each reference data files via data_file.file_path.
    """
    s3_key: str
    avro_schema: dict          # writer_schema preserved for Phase 2 round-trip writes
    records: list[dict]        # Each record has 'data_file' dict with 'file_path'


@dataclass
class IcebergMetadataGraph:
    """Full in-memory representation of a loaded Iceberg table's metadata.

    The metadata dict is the raw parsed metadata.json.
    manifest_lists and manifests are loaded from the Avro files referenced
    in the current snapshot.
    """
    metadata_s3_key: str
    metadata: dict             # Raw parsed metadata.json
    manifest_lists: list[ManifestListFile] = field(default_factory=list)
    manifests: list[ManifestFile] = field(default_factory=list)
