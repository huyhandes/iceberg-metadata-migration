"""Glue catalog registrar module for idempotent Iceberg table registration.

Provides:
  - derive_glue_names: Extract (database, table) from an S3 table location URI
  - register_or_update: Register a new Glue table or update metadata_location if it exists

Design decisions:
  - D-05: Creates Iceberg-specific Glue table properties (table_type=ICEBERG, metadata_location)
  - D-06: Idempotent re-runs: catches AlreadyExistsException and updates metadata_location
  - D-07: Glue database/table names can be derived from table_location path (Hive-style layout)
  - Does NOT create databases: raises a clear error if the Glue database does not exist (Pitfall 3)
  - VersionId passed to update_table for optimistic locking (avoids Glue version limit with SkipArchive=True)

Note on implementation approach: We use the boto3 Glue client directly (rather than
PyIceberg's GlueCatalog.register_table) because register_table requires reading the
metadata file via S3 FileIO (needs s3fs or pyarrow installed). The direct Glue API approach
sets the same Iceberg-specific Parameters that PyIceberg would set (table_type, metadata_location),
ensuring Athena and EMR compatibility while removing the FileIO dependency.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_glue.type_defs import TableInputTypeDef
    from pyiceberg.catalog.glue import GlueCatalog

from iceberg_migrate.s3 import parse_s3_uri


def derive_glue_names(table_location: str) -> tuple[str, str]:
    """Derive (glue_database, glue_table) from an S3 table location URI.

    Assumes Hive-style warehouse layout: s3://bucket/warehouse/<database>/<table>
    Falls back to ("default", last_segment) if the path has fewer than 2 segments.

    Args:
        table_location: S3 URI of the Iceberg table root (e.g., s3://bucket/warehouse/db/table)

    Returns:
        Tuple of (glue_database, glue_table)

    Raises:
        ValueError: If the table_location cannot be parsed as an S3 URI or has no path segments
    """
    _, key = parse_s3_uri(table_location)
    parts = key.rstrip("/").split("/")
    # Filter out empty parts (e.g., from leading slashes already stripped by parse_s3_uri)
    parts = [p for p in parts if p]

    if len(parts) >= 3:
        # Hive-style: warehouse/<database>/<table> — use second-to-last as database
        return parts[-2], parts[-1]
    elif len(parts) == 2:
        # Only two path segments (e.g., warehouse/<table>) — no database component; use "default"
        return "default", parts[-1]
    elif len(parts) == 1:
        return "default", parts[-1]
    else:
        raise ValueError(f"Cannot derive Glue names from table_location: {table_location!r}")


ICEBERG_TYPE_MAP = {
    "boolean": "boolean",
    "int": "int",
    "long": "bigint",
    "float": "float",
    "double": "double",
    "decimal": "decimal",
    "date": "date",
    "time": "string",
    "timestamp": "timestamp",
    "timestamptz": "timestamp",
    "string": "string",
    "uuid": "string",
    "fixed": "binary",
    "binary": "binary",
}


def _iceberg_schema_to_glue_columns(metadata: dict) -> list[dict]:
    """Convert Iceberg schema fields to Glue Columns format with iceberg.field.* parameters.

    Athena requires these parameters to correctly resolve Iceberg columns.
    """
    schemas = metadata.get("schemas", [])
    current_schema_id = metadata.get("current-schema-id", 0)
    schema = next((s for s in schemas if s.get("schema-id") == current_schema_id), schemas[0] if schemas else None)
    if not schema:
        return []

    columns = []
    for field in schema.get("fields", []):
        field_type = field.get("type", "string")
        if isinstance(field_type, dict):
            field_type = field_type.get("type", "string")
        glue_type = ICEBERG_TYPE_MAP.get(field_type, "string")
        columns.append({
            "Name": field["name"],
            "Type": glue_type,
            "Parameters": {
                "iceberg.field.id": str(field.get("id", 0)),
                "iceberg.field.optional": str(not field.get("required", False)).lower(),
                "iceberg.field.current": "true",
            },
        })
    return columns


def register_or_update(
    _catalog: GlueCatalog | None,
    glue_client: GlueClient,
    database: str,
    table: str,
    metadata_s3_uri: str,
    metadata: dict | None = None,
) -> str:
    """Register a table in Glue Catalog, or update metadata_location if it already exists.

    Per D-05: Sets Iceberg-specific Glue table properties (table_type=ICEBERG, metadata_location).
    Per D-06: Catches AlreadyExistsException to implement idempotent re-runs.

    On update, preserves StorageDescriptor and PartitionKeys from the existing Glue table,
    updating only metadata_location and table_type in Parameters. Uses VersionId for
    optimistic locking and SkipArchive=True to prevent Glue version limit accumulation.

    Does NOT create databases — raises a clear error if the database does not exist.

    Args:
        catalog: PyIceberg GlueCatalog instance (reserved for interface compatibility).
        glue_client: A boto3 Glue client (same region as catalog).
        database: Glue database name.
        table: Glue table name.
        metadata_s3_uri: Full S3 URI to the metadata.json file (not the metadata/ directory).

    Returns:
        "created" if a new table was registered, "updated" if an existing table was updated.

    Raises:
        Exception: Any non-AlreadyExists Glue exception propagates to the caller.
    """
    try:
        # Create new Glue table with Iceberg-specific properties (D-05)
        # Extract table location from metadata URI (strip /metadata/XXX.metadata.json)
        table_s3_location = metadata_s3_uri.rsplit("/metadata/", 1)[0].replace("/_migrated", "")
        columns = _iceberg_schema_to_glue_columns(metadata) if metadata else []
        create_input = cast("TableInputTypeDef", {
            "Name": table,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "table_type": "ICEBERG",
                "metadata_location": metadata_s3_uri,
            },
            "StorageDescriptor": {
                "Location": table_s3_location,
                "AdditionalLocations": [f"{table_s3_location}/data"],
                "Columns": columns,
                "Compressed": False,
                "InputFormat": "org.apache.hadoop.mapred.FileInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                },
            },
            "PartitionKeys": [],
        })
        glue_client.create_table(DatabaseName=database, TableInput=create_input)
        return "created"
    except glue_client.exceptions.AlreadyExistsException:
        # Retrieve current Glue table to get VersionId for optimistic locking (D-06)
        response = glue_client.get_table(DatabaseName=database, Name=table)
        glue_table = response["Table"]
        version_id = glue_table.get("VersionId", "0")

        # Reconstruct TableInput: preserve existing StorageDescriptor and PartitionKeys,
        # only update metadata_location and table_type in Parameters
        update_input = cast("TableInputTypeDef", {
            "Name": table,
            "StorageDescriptor": glue_table.get("StorageDescriptor", {}),
            "PartitionKeys": glue_table.get("PartitionKeys", []),
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                **glue_table.get("Parameters", {}),
                "metadata_location": metadata_s3_uri,
                "table_type": "ICEBERG",
            },
        })
        glue_client.update_table(
            DatabaseName=database,
            TableInput=update_input,
            SkipArchive=True,
            VersionId=version_id,
        )
        return "updated"
