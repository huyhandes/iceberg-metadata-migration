"""REST catalog registrar using Iceberg REST API spec.

Supports all REST-spec compliant catalogs (LakeKeeper, Polaris, Gravitino, Nessie)
via the POST /v1/{prefix}/namespaces/{namespace}/register endpoint.

Authentication:
  - OAuth2 client credentials (via credential param)
  - Bearer token (via token param)
  - No auth (for local dev)
"""

from __future__ import annotations

from typing import Any

import httpx

from iceberg_migrate.catalog.base import (
    AuthenticationError,
    CatalogConfig,
    CatalogError,
    CatalogUnreachableError,
    NamespaceNotFoundError,
    TableAlreadyExistsError,
)


class RestRegistrar:
    """Register tables with any Iceberg REST-catalog-compatible service.

    Uses the standard Iceberg REST Catalog API:
      POST /v1/{prefix}/namespaces/{namespace}/register
    with body: {"name": "<table>", "metadata_location": "<s3-uri>"}
    """

    def __init__(self, config: CatalogConfig):
        errors = config.validate()
        if errors:
            raise CatalogError(
                f"Invalid REST catalog config: {'; '.join(errors)}", "rest"
            )
        self._config: CatalogConfig = config
        self._base_url: str = (config.uri or "").rstrip("/")
        self._prefix: str = config.warehouse or ""
        self._headers: dict[str, str] = self._build_headers()

    def _build_headers(self) -> dict[str, str]:
        """Build HTTP headers including auth."""
        headers: dict[str, str] = {
            "Content-Type": "application/json",
        }
        if self._config.token:
            headers["Authorization"] = f"Bearer {self._config.token}"
        for k, v in self._config.properties.items():
            if k.startswith("header."):
                headers[k[len("header.") :]] = v
        return headers

    @property
    def _v1_base(self) -> str:
        """Build the v1 base URL with optional prefix."""
        base = f"{self._base_url}/v1"
        if self._prefix:
            base = f"{base}/{self._prefix}"
        return base

    def register_table(
        self,
        namespace: str,
        table: str,
        metadata_location: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Register a table via the Iceberg REST register endpoint.

        Args:
            namespace: Catalog namespace.
            table: Table name.
            metadata_location: S3 URI to metadata.json.
            metadata: Unused for REST (catalog reads metadata itself).

        Returns:
            "created" on success.

        Raises:
            TableAlreadyExistsError: If table already exists (HTTP 409).
            NamespaceNotFoundError: If namespace doesn't exist (HTTP 404).
            AuthenticationError: If auth fails (HTTP 401/403).
            CatalogUnreachableError: If endpoint is unreachable.
            CatalogError: For any other failure.
        """
        url = f"{self._v1_base}/namespaces/{namespace}/register"
        body = {
            "name": table,
            "metadata_location": metadata_location,
        }

        try:
            resp = httpx.post(url, json=body, headers=self._headers, timeout=30.0)
        except httpx.ConnectError as exc:
            raise CatalogUnreachableError(
                f"Cannot reach REST catalog at {self._base_url}: {exc}", "rest"
            ) from exc
        except httpx.TimeoutException as exc:
            raise CatalogUnreachableError(
                f"REST catalog timed out at {self._base_url}: {exc}", "rest"
            ) from exc

        if resp.status_code == 200:
            return "created"
        elif resp.status_code == 409:
            raise TableAlreadyExistsError(
                f"Table {namespace}.{table} already exists in REST catalog", "rest"
            )
        elif resp.status_code == 404:
            raise NamespaceNotFoundError(
                f"Namespace '{namespace}' not found in REST catalog", "rest"
            )
        elif resp.status_code in (401, 403):
            raise AuthenticationError(
                f"Authentication failed for REST catalog (HTTP {resp.status_code}): {resp.text}",
                "rest",
            )
        else:
            raise CatalogError(
                f"REST catalog returned HTTP {resp.status_code}: {resp.text}", "rest"
            )

    def validate_connection(self) -> bool:
        """Check catalog connectivity via GET /v1/config.

        Returns:
            True if catalog is reachable.

        Raises:
            CatalogUnreachableError: If endpoint is unreachable.
            AuthenticationError: If auth fails.
        """
        url = f"{self._base_url}/v1/config"
        try:
            resp = httpx.get(url, headers=self._headers, timeout=10.0)
        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            raise CatalogUnreachableError(
                f"Cannot reach REST catalog at {self._base_url}: {exc}", "rest"
            ) from exc

        if resp.status_code in (401, 403):
            raise AuthenticationError(
                f"Authentication failed for REST catalog (HTTP {resp.status_code})",
                "rest",
            )

        return resp.status_code == 200


class GlueAdapter:
    """Glue catalog adapter wrapping the existing glue_registrar module.

    Adapts the existing register_or_update function to the CatalogRegistrar protocol.
    """

    def __init__(self, config: CatalogConfig):
        import boto3

        errors = config.validate()
        if errors:
            from iceberg_migrate.catalog.base import CatalogError

            raise CatalogError(
                f"Invalid Glue catalog config: {'; '.join(errors)}", "glue"
            )

        region = config.region or "us-east-1"
        self._glue_client: Any = boto3.client("glue", region_name=region)
        self._config: CatalogConfig = config

    def register_table(
        self,
        namespace: str,
        table: str,
        metadata_location: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Register or update a table in Glue Catalog.

        Uses the existing register_or_update function for idempotent behavior.
        """
        from iceberg_migrate.catalog.glue_registrar import register_or_update

        return register_or_update(
            None, self._glue_client, namespace, table, metadata_location, metadata
        )

    def validate_connection(self) -> bool:
        """Validate Glue connectivity by listing databases."""
        try:
            self._glue_client.get_databases()
            return True
        except Exception:
            return False
