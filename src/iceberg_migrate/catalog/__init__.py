"""Catalog adapters for Iceberg table registration."""

from iceberg_migrate.catalog.base import (
    CatalogConfig,
    CatalogError,
    CatalogRegistrar,
    TableAlreadyExistsError,
    NamespaceNotFoundError,
    CatalogUnreachableError,
    AuthenticationError,
)

__all__ = [
    "CatalogConfig",
    "CatalogError",
    "CatalogRegistrar",
    "TableAlreadyExistsError",
    "NamespaceNotFoundError",
    "CatalogUnreachableError",
    "AuthenticationError",
]
