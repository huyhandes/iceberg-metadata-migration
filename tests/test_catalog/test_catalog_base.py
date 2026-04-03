"""Tests for catalog/base.py — CatalogConfig validation, CatalogRegistrar protocol.

Covers:
  - CatalogConfig.validate() with valid/invalid configs
  - CatalogRegistrar protocol is runtime-checkable
  - Exception hierarchy
"""

from iceberg_migrate.catalog.base import (
    CatalogConfig,
    CatalogError,
    CatalogRegistrar,
    TableAlreadyExistsError,
    NamespaceNotFoundError,
    CatalogUnreachableError,
    AuthenticationError,
)


# =========================================================================
# CatalogConfig validation
# =========================================================================


class TestCatalogConfigValidation:
    def test_valid_glue_config(self):
        cfg = CatalogConfig(catalog_type="glue")
        assert cfg.validate() == []

    def test_valid_rest_config_with_uri(self):
        cfg = CatalogConfig(catalog_type="rest", uri="http://localhost:8181/iceberg")
        assert cfg.validate() == []

    def test_rest_config_without_uri_is_invalid(self):
        cfg = CatalogConfig(catalog_type="rest")
        errors = cfg.validate()
        assert len(errors) == 1
        assert "uri is required" in errors[0]

    def test_empty_catalog_type_is_invalid(self):
        cfg = CatalogConfig(catalog_type="")
        errors = cfg.validate()
        assert any("catalog_type is required" in e for e in errors)

    def test_unknown_catalog_type_is_invalid(self):
        cfg = CatalogConfig(catalog_type="cassandra")
        errors = cfg.validate()
        assert any("Unknown catalog_type" in e for e in errors)

    def test_glue_config_does_not_require_uri(self):
        cfg = CatalogConfig(catalog_type="glue", region="us-west-2")
        assert cfg.validate() == []

    def test_rest_config_with_all_fields(self):
        cfg = CatalogConfig(
            catalog_type="rest",
            uri="http://localhost:8181/iceberg",
            warehouse="my-warehouse",
            credential="client-id:client-secret",
            token="bearer-token",
            properties={"header.X-Custom": "value"},
        )
        assert cfg.validate() == []

    def test_default_properties_is_empty_dict(self):
        cfg = CatalogConfig(catalog_type="glue")
        assert cfg.properties == {}

    def test_optional_fields_default_to_none(self):
        cfg = CatalogConfig(catalog_type="glue")
        assert cfg.uri is None
        assert cfg.warehouse is None
        assert cfg.credential is None
        assert cfg.token is None
        assert cfg.region is None


# =========================================================================
# CatalogRegistrar protocol
# =========================================================================


class TestCatalogRegistrarProtocol:
    def test_concrete_class_satisfies_protocol(self):
        """A class implementing register_table + validate_connection satisfies the protocol."""

        class FakeRegistrar:
            def register_table(
                self, namespace, table, metadata_location, metadata=None
            ):
                return "created"

            def validate_connection(self):
                return True

        assert isinstance(FakeRegistrar(), CatalogRegistrar)

    def test_incomplete_class_does_not_satisfy_protocol(self):
        """A class missing methods does NOT satisfy the protocol."""

        class Incomplete:
            def register_table(
                self, namespace, table, metadata_location, metadata=None
            ):
                return "created"

            # Missing validate_connection

        assert not isinstance(Incomplete(), CatalogRegistrar)


# =========================================================================
# Exception hierarchy
# =========================================================================


class TestExceptionHierarchy:
    def test_catalog_error_has_catalog_type(self):
        err = CatalogError("test error", catalog_type="rest")
        assert err.catalog_type == "rest"
        assert str(err) == "test error"

    def test_table_already_exists_is_catalog_error(self):
        err = TableAlreadyExistsError("exists", catalog_type="rest")
        assert isinstance(err, CatalogError)
        assert err.catalog_type == "rest"

    def test_namespace_not_found_is_catalog_error(self):
        err = NamespaceNotFoundError("no ns", catalog_type="rest")
        assert isinstance(err, CatalogError)

    def test_catalog_unreachable_is_catalog_error(self):
        err = CatalogUnreachableError("unreachable", catalog_type="rest")
        assert isinstance(err, CatalogError)

    def test_authentication_error_is_catalog_error(self):
        err = AuthenticationError("bad auth", catalog_type="rest")
        assert isinstance(err, CatalogError)

    def test_default_catalog_type_is_unknown(self):
        err = CatalogError("msg")
        assert err.catalog_type == "unknown"
