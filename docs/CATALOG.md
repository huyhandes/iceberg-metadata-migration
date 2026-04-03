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
