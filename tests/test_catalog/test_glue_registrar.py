"""Tests for Glue catalog registrar module.

Tests cover:
  - register_or_update creates a new Glue table (returns "created")
  - register_or_update updates existing table metadata_location (returns "updated")
  - register_or_update preserves existing table properties on re-run
  - derive_glue_names extracts (database, table) from a deep S3 path
  - derive_glue_names falls back to ("default", table) for shallow S3 paths
"""
import pytest

from iceberg_migrate.catalog.glue_registrar import register_or_update, derive_glue_names


class TestDeriveGlueNames:
    """Tests for derive_glue_names path parsing."""

    def test_deep_path_extracts_db_and_table(self):
        """s3://bucket/warehouse/mydb/mytable -> ("mydb", "mytable")"""
        db, table = derive_glue_names("s3://bucket/warehouse/mydb/mytable")
        assert db == "mydb"
        assert table == "mytable"

    def test_shallow_path_falls_back_to_default_db(self):
        """s3://bucket/warehouse/mytable -> ("default", "mytable")"""
        db, table = derive_glue_names("s3://bucket/warehouse/mytable")
        assert db == "default"
        assert table == "mytable"


class TestRegisterOrUpdate:
    """Tests for register_or_update idempotent Glue registration."""

    def test_register_new_table_returns_created(self, aws_clients):
        """register_or_update creates a new Glue table and returns 'created'."""
        s3, glue = aws_clients
        # catalog parameter is reserved for interface compatibility; pass None in tests
        result = register_or_update(
            None,
            glue,
            "testdb",
            "mytable",
            "s3://mybucket/warehouse/testdb/mytable/metadata/v1.metadata.json",
        )

        assert result == "created"
        # Verify table was created in Glue
        response = glue.get_table(DatabaseName="testdb", Name="mytable")
        params = response["Table"]["Parameters"]
        assert params["metadata_location"] == "s3://mybucket/warehouse/testdb/mytable/metadata/v1.metadata.json"
        assert params["table_type"].upper() == "ICEBERG"

    def test_register_existing_table_returns_updated(self, aws_clients):
        """register_or_update updates metadata_location when table already exists, returns 'updated'."""
        s3, glue = aws_clients

        # First registration
        register_or_update(
            None,
            glue,
            "testdb",
            "mytable",
            "s3://mybucket/warehouse/testdb/mytable/metadata/v1.metadata.json",
        )

        # Second registration with updated metadata location
        result = register_or_update(
            None,
            glue,
            "testdb",
            "mytable",
            "s3://mybucket/warehouse/testdb/mytable/metadata/v2.metadata.json",
        )

        assert result == "updated"
        # Verify metadata_location was updated
        response = glue.get_table(DatabaseName="testdb", Name="mytable")
        params = response["Table"]["Parameters"]
        assert params["metadata_location"] == "s3://mybucket/warehouse/testdb/mytable/metadata/v2.metadata.json"

    def test_update_preserves_existing_table_properties(self, aws_clients):
        """register_or_update preserves existing table properties while updating metadata_location."""
        s3, glue = aws_clients

        # First registration
        register_or_update(
            None,
            glue,
            "testdb",
            "mytable",
            "s3://mybucket/warehouse/testdb/mytable/metadata/v1.metadata.json",
        )

        # Get the initial table properties
        response_before = glue.get_table(DatabaseName="testdb", Name="mytable")
        initial_storage_descriptor = response_before["Table"].get("StorageDescriptor", {})
        initial_partition_keys = response_before["Table"].get("PartitionKeys", [])

        # Second registration
        register_or_update(
            None,
            glue,
            "testdb",
            "mytable",
            "s3://mybucket/warehouse/testdb/mytable/metadata/v2.metadata.json",
        )

        # Verify StorageDescriptor and PartitionKeys are preserved
        response_after = glue.get_table(DatabaseName="testdb", Name="mytable")
        assert response_after["Table"].get("StorageDescriptor", {}) == initial_storage_descriptor
        assert response_after["Table"].get("PartitionKeys", []) == initial_partition_keys
        # table_type should still be ICEBERG
        assert response_after["Table"]["Parameters"]["table_type"].upper() == "ICEBERG"
