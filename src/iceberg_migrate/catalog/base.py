"""Catalog abstraction layer for pluggable Iceberg catalog registration.

Defines the CatalogRegistrar protocol and CatalogConfig dataclass that all
catalog adapters (Glue, REST, etc.) must implement.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass
class CatalogConfig:
    """Configuration for a target catalog adapter.

    Attributes:
        catalog_type: Adapter identifier ('glue', 'rest').
        uri: REST catalog URI (required for REST catalogs).
        warehouse: Warehouse identifier or path.
        credential: OAuth2 client credential string (client-id:client-secret).
        token: Bearer token for authentication.
        region: AWS region (Glue/REST with SigV4).
        properties: Additional adapter-specific properties.
    """

    catalog_type: str
    uri: str | None = None
    warehouse: str | None = None
    credential: str | None = None
    token: str | None = None
    region: str | None = None
    properties: dict[str, str] = field(default_factory=dict)

    def validate(self) -> list[str]:
        """Validate config and return list of errors (empty if valid)."""
        errors: list[str] = []
        if not self.catalog_type:
            errors.append("catalog_type is required")
        if self.catalog_type == "rest" and not self.uri:
            errors.append("uri is required for REST catalogs")
        if self.catalog_type not in ("glue", "rest"):
            errors.append(
                f"Unknown catalog_type '{self.catalog_type}'. Must be 'glue' or 'rest'."
            )
        return errors


@runtime_checkable
class CatalogRegistrar(Protocol):
    """Protocol that all catalog adapters must implement.

    Each adapter handles registering an Iceberg table with a specific
    catalog backend (Glue, REST-based catalogs like LakeKeeper/Polaris/etc.).
    """

    def register_table(
        self,
        namespace: str,
        table: str,
        metadata_location: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Register a table in the catalog.

        Args:
            namespace: Catalog namespace/database name.
            table: Table name.
            metadata_location: S3 URI to the metadata.json file.
            metadata: Optional parsed metadata dict.

        Returns:
            "created" if a new table was registered, "updated" if existing.

        Raises:
            CatalogError: On any catalog operation failure.
        """
        ...

    def validate_connection(self) -> bool:
        """Validate catalog connectivity and auth.

        Returns:
            True if the catalog is reachable and auth is valid.

        Raises:
            CatalogError: If connectivity or auth fails.
        """
        ...


class CatalogError(Exception):
    """Base exception for catalog operations."""

    def __init__(self, message: str, catalog_type: str = "unknown"):
        super().__init__(message)
        self.catalog_type = catalog_type


class TableAlreadyExistsError(CatalogError):
    """Raised when a table already exists and cannot be overwritten."""

    pass


class NamespaceNotFoundError(CatalogError):
    """Raised when the target namespace does not exist."""

    pass


class CatalogUnreachableError(CatalogError):
    """Raised when the catalog endpoint is unreachable."""

    pass


class AuthenticationError(CatalogError):
    """Raised when authentication to the catalog fails."""

    pass
