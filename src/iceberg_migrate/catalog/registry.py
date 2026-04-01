"""Catalog registry: factory for creating catalog adapters by type.

Provides get_registrar() to instantiate the correct adapter based on
CatalogConfig.catalog_type. Supports auto-detection from URI patterns.
"""
from __future__ import annotations

from typing import Any

from iceberg_migrate.catalog.base import (
    CatalogConfig,
    CatalogError,
    CatalogRegistrar,
)


def detect_catalog_type(uri: str) -> str:
    """Auto-detect catalog type from a URI pattern.

    Args:
        uri: Catalog URI string.

    Returns:
        "glue" for Glue REST URIs, "rest" for everything else.
    """
    if "glue" in uri.lower() and "amazonaws.com" in uri.lower():
        return "glue"
    if uri.startswith("s3://") and "glue" not in uri.lower():
        # Plain S3 URI without REST catalog — default to glue
        return "glue"
    return "rest"


def get_registrar(config: CatalogConfig) -> CatalogRegistrar:
    """Create and return the appropriate catalog registrar.

    Args:
        config: CatalogConfig with catalog_type and auth details.

    Returns:
        A CatalogRegistrar instance for the configured catalog.

    Raises:
        CatalogError: If catalog_type is unknown or config is invalid.
    """
    errors = config.validate()
    if errors:
        raise CatalogError(f"Invalid catalog config: {'; '.join(errors)}")

    if config.catalog_type == "glue":
        from iceberg_migrate.catalog.rest_registrar import GlueAdapter
        return GlueAdapter(config)
    elif config.catalog_type == "rest":
        from iceberg_migrate.catalog.rest_registrar import RestRegistrar
        return RestRegistrar(config)
    else:
        raise CatalogError(f"Unknown catalog type: {config.catalog_type}")
