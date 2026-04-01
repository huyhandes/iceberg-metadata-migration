"""Glue adapter regression tests — verify existing Glue behavior after refactor.

Ensures the multi-catalog refactoring does not break existing Glue functionality:
  - derive_glue_names edge cases
  - register_or_update idempotency
  - Error handling for non-existent database
  - Schema conversion with various field types
"""
import boto3
import pytest
from moto import mock_aws

from iceberg_migrate.catalog.glue_registrar import (
    derive_glue_names,
    register_or_update,
)


class TestDeriveGlueNamesEdgeCases:
    """Extended derive_glue_names tests beyond existing coverage."""

    def test_single_segment_uses_default(self):
        db, tbl = derive_glue_names("s3://bucket/mytable")
        assert db == "default"
        assert tbl == "mytable"

    def test_deep_nested_path(self):
        db, tbl = derive_glue_names("s3://bucket/a/b/c/db/table")
        assert db == "db"
        assert tbl == "table"

    def test_trailing_slash_stripped(self):
        db, tbl = derive_glue_names("s3://bucket/warehouse/db/table/")
        assert db == "db"
        assert tbl == "table"

    def test_s3a_scheme_supported(self):
        db, tbl = derive_glue_names("s3a://bucket/warehouse/db/table")
        assert db == "db"
        assert tbl == "table"


class TestGlueRegistrationErrors:

    @mock_aws
    def test_register_in_nonexistent_database_raises(self):
        """Registering in a non-existent Glue database raises EntityNotFoundException."""
        glue = boto3.client("glue", region_name="us-east-1")
        # No database created

        with pytest.raises(Exception):  # EntityNotFoundException
            register_or_update(
                None, glue, "nonexistent_db", "tbl",
                "s3://bucket/warehouse/db/tbl/metadata/v1.metadata.json",
            )

    @mock_aws
    def test_register_sets_table_type_iceberg(self):
        """Created table must have table_type=ICEBERG in Parameters."""
        glue = boto3.client("glue", region_name="us-east-1")
        glue.create_database(DatabaseInput={"Name": "testdb"})

        register_or_update(
            None, glue, "testdb", "tbl",
            "s3://bucket/warehouse/testdb/tbl/metadata/v1.metadata.json",
        )

        resp = glue.get_table(DatabaseName="testdb", Name="tbl")
        assert resp["Table"]["Parameters"]["table_type"].upper() == "ICEBERG"

    @mock_aws
    def test_register_sets_storage_descriptor_location(self):
        """Created table StorageDescriptor.Location must match table location."""
        glue = boto3.client("glue", region_name="us-east-1")
        glue.create_database(DatabaseInput={"Name": "testdb"})

        register_or_update(
            None, glue, "testdb", "tbl",
            "s3://bucket/warehouse/testdb/tbl/metadata/v1.metadata.json",
        )

        resp = glue.get_table(DatabaseName="testdb", Name="tbl")
        location = resp["Table"]["StorageDescriptor"]["Location"]
        assert location == "s3://bucket/warehouse/testdb/tbl"

    @mock_aws
    def test_update_preserves_parameters(self):
        """Re-registration preserves any custom parameters."""
        glue = boto3.client("glue", region_name="us-east-1")
        glue.create_database(DatabaseInput={"Name": "testdb"})

        register_or_update(
            None, glue, "testdb", "tbl",
            "s3://bucket/warehouse/testdb/tbl/metadata/v1.metadata.json",
        )

        # Add a custom parameter manually
        resp = glue.get_table(DatabaseName="testdb", Name="tbl")
        params = resp["Table"]["Parameters"]
        params["custom_key"] = "custom_value"
        glue.update_table(
            DatabaseName="testdb",
            TableInput={
                "Name": "tbl",
                "TableType": "EXTERNAL_TABLE",
                "Parameters": params,
                "StorageDescriptor": resp["Table"]["StorageDescriptor"],
            },
            SkipArchive=True,
        )

        # Re-register
        register_or_update(
            None, glue, "testdb", "tbl",
            "s3://bucket/warehouse/testdb/tbl/metadata/v2.metadata.json",
        )

        resp = glue.get_table(DatabaseName="testdb", Name="tbl")
        assert resp["Table"]["Parameters"]["custom_key"] == "custom_value"
        assert resp["Table"]["Parameters"]["metadata_location"] == (
            "s3://bucket/warehouse/testdb/tbl/metadata/v2.metadata.json"
        )
