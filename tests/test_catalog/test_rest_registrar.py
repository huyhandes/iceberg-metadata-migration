"""Tests for catalog/rest_registrar.py — REST catalog adapter and Glue adapter.

Covers:
  - RestRegistrar: register_table with mocked HTTP responses
  - Auth scenarios (OAuth2/bearer/no auth)
  - Error handling (409 conflict, 404 namespace, 401/403 auth, unreachable, timeout)
  - validate_connection via GET /v1/config
  - REST spec compliance (correct HTTP methods, paths, payloads)
  - GlueAdapter: wraps existing glue_registrar (regression test)
"""

from unittest.mock import MagicMock, patch

import pytest

from iceberg_migrate.catalog.base import (
    AuthenticationError,
    CatalogConfig,
    CatalogError,
    CatalogUnreachableError,
    NamespaceNotFoundError,
    TableAlreadyExistsError,
)
from iceberg_migrate.catalog.rest_registrar import RestRegistrar, GlueAdapter


# =========================================================================
# Helpers
# =========================================================================


def _rest_config(**overrides) -> CatalogConfig:
    defaults = dict(
        catalog_type="rest",
        uri="http://localhost:8181/iceberg",
    )
    defaults.update(overrides)
    return CatalogConfig(**defaults)


def _mock_response(status_code=200, text="", json_data=None):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    if json_data is not None:
        resp.json.return_value = json_data
    return resp


# =========================================================================
# RestRegistrar: register_table
# =========================================================================


class TestRestRegisterTable:
    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_register_table_success_returns_created(self, mock_post):
        """POST register returns 200 → 'created'."""
        mock_post.return_value = _mock_response(200)
        registrar = RestRegistrar(_rest_config())
        result = registrar.register_table(
            "mydb", "mytable", "s3://bucket/metadata/v1.metadata.json"
        )
        assert result == "created"

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_register_table_sends_correct_url(self, mock_post):
        """Verify the URL follows REST spec: /v1/{prefix}/namespaces/{ns}/register."""
        mock_post.return_value = _mock_response(200)
        registrar = RestRegistrar(_rest_config(warehouse="wh1"))
        registrar.register_table("ns1", "tbl1", "s3://b/m.json")

        call_args = mock_post.call_args
        url = call_args[1].get("url") or call_args[0][0]
        assert "/v1/wh1/namespaces/ns1/register" in url

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_register_table_sends_correct_payload(self, mock_post):
        """Body must contain 'name' and 'metadata_location' per REST spec."""
        mock_post.return_value = _mock_response(200)
        registrar = RestRegistrar(_rest_config())
        registrar.register_table("ns1", "tbl1", "s3://b/m.json")

        call_args = mock_post.call_args
        body = call_args[1].get("json") or call_args[0][1]
        assert body["name"] == "tbl1"
        assert body["metadata_location"] == "s3://b/m.json"

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_register_table_without_warehouse_no_prefix(self, mock_post):
        """Without warehouse, URL is /v1/namespaces/{ns}/register."""
        mock_post.return_value = _mock_response(200)
        registrar = RestRegistrar(_rest_config())
        registrar.register_table("ns1", "tbl1", "s3://b/m.json")

        url = mock_post.call_args[1].get("url") or mock_post.call_args[0][0]
        assert "/v1/namespaces/ns1/register" in url

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_register_table_409_raises_table_already_exists(self, mock_post):
        """HTTP 409 → TableAlreadyExistsError."""
        mock_post.return_value = _mock_response(409, text="Table already exists")
        registrar = RestRegistrar(_rest_config())

        with pytest.raises(TableAlreadyExistsError) as exc_info:
            registrar.register_table("ns1", "tbl1", "s3://b/m.json")
        assert "already exists" in str(exc_info.value).lower()

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_register_table_404_raises_namespace_not_found(self, mock_post):
        """HTTP 404 → NamespaceNotFoundError."""
        mock_post.return_value = _mock_response(404, text="Namespace not found")
        registrar = RestRegistrar(_rest_config())

        with pytest.raises(NamespaceNotFoundError):
            registrar.register_table("missing_ns", "tbl1", "s3://b/m.json")

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_register_table_401_raises_authentication_error(self, mock_post):
        """HTTP 401 → AuthenticationError."""
        mock_post.return_value = _mock_response(401, text="Unauthorized")
        registrar = RestRegistrar(_rest_config())

        with pytest.raises(AuthenticationError):
            registrar.register_table("ns1", "tbl1", "s3://b/m.json")

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_register_table_403_raises_authentication_error(self, mock_post):
        """HTTP 403 → AuthenticationError."""
        mock_post.return_value = _mock_response(403, text="Forbidden")
        registrar = RestRegistrar(_rest_config())

        with pytest.raises(AuthenticationError):
            registrar.register_table("ns1", "tbl1", "s3://b/m.json")

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_register_table_500_raises_catalog_error(self, mock_post):
        """HTTP 500 → generic CatalogError."""
        mock_post.return_value = _mock_response(500, text="Internal Server Error")
        registrar = RestRegistrar(_rest_config())

        with pytest.raises(CatalogError) as exc_info:
            registrar.register_table("ns1", "tbl1", "s3://b/m.json")
        assert "500" in str(exc_info.value)


# =========================================================================
# RestRegistrar: connectivity / unreachable
# =========================================================================


class TestRestConnectivity:
    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_connection_refused_raises_unreachable(self, mock_post):
        """Connection refused → CatalogUnreachableError."""
        import httpx

        mock_post.side_effect = httpx.ConnectError("Connection refused")
        registrar = RestRegistrar(_rest_config())

        with pytest.raises(CatalogUnreachableError):
            registrar.register_table("ns1", "tbl1", "s3://b/m.json")

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_timeout_raises_unreachable(self, mock_post):
        """Request timeout → CatalogUnreachableError."""
        import httpx

        mock_post.side_effect = httpx.TimeoutException("Timed out")
        registrar = RestRegistrar(_rest_config())

        with pytest.raises(CatalogUnreachableError):
            registrar.register_table("ns1", "tbl1", "s3://b/m.json")


# =========================================================================
# RestRegistrar: authentication scenarios
# =========================================================================


class TestRestAuthScenarios:
    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_bearer_token_sent_in_header(self, mock_post):
        """Token config → Authorization: Bearer <token> header."""
        mock_post.return_value = _mock_response(200)
        cfg = _rest_config(token="my-bearer-token")
        registrar = RestRegistrar(cfg)
        registrar.register_table("ns1", "tbl1", "s3://b/m.json")

        headers = mock_post.call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer my-bearer-token"

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_no_auth_sends_no_authorization_header(self, mock_post):
        """Config without token/credential → no Authorization header."""
        mock_post.return_value = _mock_response(200)
        registrar = RestRegistrar(_rest_config())
        registrar.register_table("ns1", "tbl1", "s3://b/m.json")

        headers = mock_post.call_args[1]["headers"]
        assert "Authorization" not in headers

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_custom_headers_from_properties(self, mock_post):
        """Properties with 'header.' prefix are sent as HTTP headers."""
        mock_post.return_value = _mock_response(200)
        cfg = _rest_config(
            properties={"header.X-Iceberg-Access-Delegation": "vended-credentials"}
        )
        registrar = RestRegistrar(cfg)
        registrar.register_table("ns1", "tbl1", "s3://b/m.json")

        headers = mock_post.call_args[1]["headers"]
        assert headers["X-Iceberg-Access-Delegation"] == "vended-credentials"


# =========================================================================
# RestRegistrar: validate_connection
# =========================================================================


class TestRestValidateConnection:
    @patch("iceberg_migrate.catalog.rest_registrar.httpx.get")
    def test_validate_connection_success(self, mock_get):
        """GET /v1/config returns 200 → True."""
        mock_get.return_value = _mock_response(200)
        registrar = RestRegistrar(_rest_config())
        assert registrar.validate_connection() is True

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.get")
    def test_validate_connection_unreachable(self, mock_get):
        """GET /v1/config connection error → CatalogUnreachableError."""
        import httpx

        mock_get.side_effect = httpx.ConnectError("refused")
        registrar = RestRegistrar(_rest_config())

        with pytest.raises(CatalogUnreachableError):
            registrar.validate_connection()

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.get")
    def test_validate_connection_auth_failure(self, mock_get):
        """GET /v1/config returns 401 → AuthenticationError."""
        mock_get.return_value = _mock_response(401)
        registrar = RestRegistrar(_rest_config())

        with pytest.raises(AuthenticationError):
            registrar.validate_connection()


# =========================================================================
# RestRegistrar: REST spec compliance
# =========================================================================


class TestRestSpecCompliance:
    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_uses_post_method(self, mock_post):
        """register_table must use HTTP POST."""
        mock_post.return_value = _mock_response(200)
        registrar = RestRegistrar(_rest_config())
        registrar.register_table("ns1", "tbl1", "s3://b/m.json")
        mock_post.assert_called_once()

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_sends_json_content_type(self, mock_post):
        """Request must include Content-Type: application/json."""
        mock_post.return_value = _mock_response(200)
        registrar = RestRegistrar(_rest_config())
        registrar.register_table("ns1", "tbl1", "s3://b/m.json")

        headers = mock_post.call_args[1]["headers"]
        assert headers["Content-Type"] == "application/json"

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_request_body_has_name_and_metadata_location(self, mock_post):
        """Request body must match RegisterTableRequest schema."""
        mock_post.return_value = _mock_response(200)
        registrar = RestRegistrar(_rest_config())
        registrar.register_table("ns1", "tbl1", "s3://b/metadata/v1.metadata.json")

        body = mock_post.call_args[1]["json"]
        assert set(body.keys()) == {"name", "metadata_location"}
        assert body["name"] == "tbl1"
        assert body["metadata_location"] == "s3://b/metadata/v1.metadata.json"

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.get")
    def test_validate_uses_get_v1_config(self, mock_get):
        """validate_connection must call GET {base}/v1/config."""
        mock_get.return_value = _mock_response(200)
        registrar = RestRegistrar(_rest_config(uri="http://catalog:8181/iceberg"))
        registrar.validate_connection()

        url = mock_get.call_args[1].get("url") or mock_get.call_args[0][0]
        assert url == "http://catalog:8181/iceberg/v1/config"


# =========================================================================
# GlueAdapter: regression wrapper
# =========================================================================


class TestGlueAdapter:
    def test_glue_adapter_register_new_table(self, aws_clients):
        """GlueAdapter.register_table creates a Glue table → 'created'."""
        s3, glue = aws_clients
        cfg = CatalogConfig(catalog_type="glue", region="us-east-1")
        adapter = GlueAdapter(cfg)
        # Override the boto3 client with moto mock
        adapter._glue_client = glue

        result = adapter.register_table(
            "testdb",
            "mytable",
            "s3://b/warehouse/testdb/mytable/metadata/v1.metadata.json",
        )
        assert result == "created"

    def test_glue_adapter_register_existing_table(self, aws_clients):
        """GlueAdapter.register_table updates existing → 'updated'."""
        s3, glue = aws_clients
        cfg = CatalogConfig(catalog_type="glue", region="us-east-1")
        adapter = GlueAdapter(cfg)
        adapter._glue_client = glue

        adapter.register_table(
            "testdb",
            "mytable",
            "s3://b/warehouse/testdb/mytable/metadata/v1.metadata.json",
        )
        result = adapter.register_table(
            "testdb",
            "mytable",
            "s3://b/warehouse/testdb/mytable/metadata/v2.metadata.json",
        )
        assert result == "updated"

    def test_glue_adapter_validate_connection(self, aws_clients):
        """GlueAdapter.validate_connection returns True for reachable Glue."""
        s3, glue = aws_clients
        cfg = CatalogConfig(catalog_type="glue", region="us-east-1")
        adapter = GlueAdapter(cfg)
        adapter._glue_client = glue

        assert adapter.validate_connection() is True

    def test_invalid_glue_config_raises(self):
        """Invalid config raises CatalogError."""
        cfg = CatalogConfig(catalog_type="invalid")
        with pytest.raises(CatalogError):
            GlueAdapter(cfg)


# =========================================================================
# RestRegistrar: init validation
# =========================================================================


class TestRestRegistrarInit:
    def test_rest_without_uri_raises(self):
        """RestRegistrar with no URI raises CatalogError on init."""
        cfg = CatalogConfig(catalog_type="rest")
        with pytest.raises(CatalogError) as exc_info:
            RestRegistrar(cfg)
        assert "uri is required" in str(exc_info.value)

    @patch("iceberg_migrate.catalog.rest_registrar.httpx.post")
    def test_strips_trailing_slash_from_uri(self, mock_post):
        """URI trailing slash is stripped to prevent double-slash in paths."""
        mock_post.return_value = _mock_response(200)
        registrar = RestRegistrar(_rest_config(uri="http://host:8181/iceberg/"))
        registrar.register_table("ns1", "tbl1", "s3://b/m.json")

        url = mock_post.call_args[1].get("url") or mock_post.call_args[0][0]
        assert "//" not in url.replace("http://", "").replace("s3://", "")
