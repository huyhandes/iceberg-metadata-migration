# Catalog Abstraction

## Overview

The catalog layer provides a pluggable interface for registering migrated Iceberg tables with different catalog backends. Currently supports AWS Glue and REST-based catalogs.

## Architecture

```
catalog/
├── base.py             # CatalogRegistrar protocol + CatalogConfig + error types
├── registry.py         # Maps catalog_type string → adapter instance
├── glue_registrar.py   # AWS Glue implementation
└── rest_registrar.py   # REST catalog implementation (Polaris, LakeKeeper, etc.)
```

## CatalogRegistrar Protocol

All adapters implement this protocol (defined in `base.py`):

```python
class CatalogRegistrar(Protocol):
    def register_table(
        self,
        namespace: str,
        table: str,
        metadata_location: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:  # Returns "created" or "updated"
        ...

    def validate_connection(self) -> bool:
        ...
```

## CatalogConfig

```python
@dataclass
class CatalogConfig:
    catalog_type: str           # 'glue' or 'rest'
    uri: str | None             # REST catalog URI
    warehouse: str | None       # Warehouse identifier
    credential: str | None      # OAuth2 client credential
    token: str | None           # Bearer token
    region: str | None          # AWS region
    properties: dict[str, str]  # Additional properties
```

## AWS Glue Adapter

**Module:** `catalog/glue_registrar.py`

- Uses boto3 Glue client directly (not PyIceberg's GlueCatalog)
- Sets Iceberg-specific parameters: `table_type=ICEBERG`, `metadata_location`
- Idempotent: creates new table or updates existing via optimistic locking
- Derives database/table names from S3 path (Hive-style: `warehouse/<db>/<table>`)
- Does NOT create databases — requires pre-existing Glue database

## REST Catalog Adapter

**Module:** `catalog/rest_registrar.py`

- Implements the Iceberg REST Catalog spec
- Supports OAuth2 (`credential`) and bearer token (`token`) auth
- Compatible with Polaris, LakeKeeper, and other REST-based catalogs

## Adding a New Adapter

1. Create `catalog/<name>_registrar.py`
2. Implement the `CatalogRegistrar` protocol
3. Register in `catalog/registry.py`

Example skeleton:

```python
from iceberg_migrate.catalog.base import CatalogConfig, CatalogRegistrar

class MyRegistrar:
    def __init__(self, config: CatalogConfig):
        self.config = config

    def register_table(self, namespace, table, metadata_location, metadata=None):
        # Implementation here
        return "created"

    def validate_connection(self):
        return True
```

Then in `registry.py`, add the mapping:
```python
"my_catalog": MyRegistrar
```

## Error Types

All defined in `base.py`:

| Exception | When Raised |
|-----------|------------|
| `CatalogError` | Base exception for catalog operations |
| `TableAlreadyExistsError` | Table exists and cannot be overwritten |
| `NamespaceNotFoundError` | Target namespace/database doesn't exist |
| `CatalogUnreachableError` | Catalog endpoint unreachable |
| `AuthenticationError` | Auth to catalog fails |

## Source Catalog Format Reference

The migration tool reads metadata produced by any Iceberg catalog. Different catalogs write metadata in different formats. This table summarizes what the tool handles during discovery and rewrite.

### Metadata.json Encoding

| Source Catalog | Encoding | Filename Pattern | Notes |
|----------------|----------|------------------|-------|
| Java-based (Glue, HMS, Nessie, Polaris, Gravitino, Unity) | Plain UTF-8 JSON (default) | `{5-digit}-{UUID}.metadata.json` | Optional gzip via `write.metadata.compression-codec` |
| Java Hadoop/filesystem | Plain UTF-8 JSON (default) | `v{N}.metadata.json` | Uses `version-hint.text` for version tracking |
| Lakekeeper (Rust) | **Gzip-compressed JSON (hardcoded)** | `{5-digit}-{UUID}.gz.metadata.json` | Not configurable per-table |
| PyIceberg (SQL catalog) | Plain UTF-8 JSON (default) | `{5-digit}-{UUID}.metadata.json` | Optional gzip via table property |

### Avro Manifest Codec

| Source Catalog | `avro.codec` Header Value | Notes |
|----------------|---------------------------|-------|
| Java-based (all) | `deflate` | Default; gzip/deflate level 9. Iceberg calls it "gzip" but Avro spec name is "deflate" |
| Lakekeeper (post Jan 2026) | `deflate` | Changed from `null` to `deflate` in iceberg-rust PR #1851 |
| Lakekeeper (pre Jan 2026) | `null` | Uncompressed Avro containers |
| PyIceberg | `deflate` | Maps `"gzip"` -> `"deflate"` in Avro header |

The tool preserves the original codec on round-trip: codec is read from the Avro container header during discovery, stored on the `ManifestListFile`/`ManifestFile` model, and passed back to `fastavro.writer()` during serialization.

### Format Version Support

| Version | Supported | Key Additions |
|---------|-----------|---------------|
| 1 | Yes | Original format; no row-level deletes |
| 2 | Yes | Row-level deletes (position + equality delete files) |
| 3 | Yes | Deletion vectors, `referenced_data_file` field |
| 4+ | **Rejected** | Not yet released; `RewriteEngine` raises `ValueError` |
