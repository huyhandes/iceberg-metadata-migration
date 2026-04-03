"""Tests for catalog/registry.py — Catalog factory and auto-detection.

Covers:
  - detect_catalog_type from URI patterns
  - get_registrar factory creates correct adapter type
  - Unknown catalog type raises CatalogError
  - Invalid config raises CatalogError
"""

import pytest
from unittest.mock import MagicMock, patch

from iceberg_migrate.catalog.base import CatalogConfig, CatalogError
from iceberg_migrate.catalog.registry import detect_catalog_type, get_registrar


class TestDetectCatalogType:
    def test_glue_aws_uri_detected_as_glue(self):
        """Glue REST endpoint URI → 'glue'."""
        assert (
            detect_catalog_type("https://glue.us-east-1.amazonaws.com/iceberg")
            == "glue"
        )

    def test_plain_s3_uri_detected_as_glue(self):
        """Plain s3:// URI without REST catalog → 'glue' (backward compat)."""
        assert detect_catalog_type("s3://my-bucket/warehouse") == "glue"

    def test_localhost_rest_detected_as_rest(self):
        """localhost REST URI → 'rest'."""
        assert detect_catalog_type("http://localhost:8181/iceberg") == "rest"

    def test_lakekeeper_detected_as_rest(self):
        """LakeKeeper-style URI → 'rest'."""
        assert detect_catalog_type("http://lakekeeper:8181/iceberg") == "rest"

    def test_polaris_detected_as_rest(self):
        """Polaris-style URI → 'rest'."""
        assert detect_catalog_type("http://polaris:8182/iceberg") == "rest"

    def test_gravitino_detected_as_rest(self):
        """Gravitino-style URI → 'rest'."""
        assert detect_catalog_type("http://gravitino:9001/iceberg") == "rest"

    def test_nessie_detected_as_rest(self):
        """Nessie-style URI → 'rest'."""
        assert detect_catalog_type("http://nessie:19120/iceberg") == "rest"


class TestGetRegistrar:
    def test_glue_type_creates_glue_adapter(self):
        """get_registrar('glue') returns a GlueAdapter instance."""
        from iceberg_migrate.catalog.rest_registrar import GlueAdapter

        cfg = CatalogConfig(catalog_type="glue", region="us-east-1")
        registrar = get_registrar(cfg)
        assert isinstance(registrar, GlueAdapter)

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.get")
    def test_rest_type_creates_rest_registrar(self, mock_get):
        """get_registrar('rest') returns a RestRegistrar instance."""
        from iceberg_migrate.catalog.rest_registrar import RestRegistrar

        mock_get.return_value = MagicMock(status_code=200)
        cfg = CatalogConfig(catalog_type="rest", uri="http://localhost:8181/iceberg")
        registrar = get_registrar(cfg)
        assert isinstance(registrar, RestRegistrar)

    def test_invalid_config_raises(self):
        """Invalid config (rest without uri) raises CatalogError."""
        cfg = CatalogConfig(catalog_type="rest")
        with pytest.raises(CatalogError):
            get_registrar(cfg)

    def test_unknown_type_raises(self):
        """Unknown catalog type raises CatalogError (caught by validate)."""
        cfg = CatalogConfig(catalog_type="cassandra")
        with pytest.raises(CatalogError):
            get_registrar(cfg)
