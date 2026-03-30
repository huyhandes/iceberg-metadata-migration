"""Tests for Pydantic BaseModel migration of iceberg_migrate.models."""
from pydantic import BaseModel
import pytest

from iceberg_migrate.models import IcebergMetadataGraph, ManifestFile, ManifestListFile


# ---------------------------------------------------------------------------
# Test 1: ManifestListFile is a Pydantic BaseModel subclass
# ---------------------------------------------------------------------------
def test_manifest_list_file_is_pydantic_model():
    """ManifestListFile should be a subclass of pydantic.BaseModel."""
    assert issubclass(ManifestListFile, BaseModel)


# ---------------------------------------------------------------------------
# Test 2: ManifestFile is a Pydantic BaseModel subclass
# ---------------------------------------------------------------------------
def test_manifest_file_is_pydantic_model():
    """ManifestFile should be a subclass of pydantic.BaseModel."""
    assert issubclass(ManifestFile, BaseModel)


# ---------------------------------------------------------------------------
# Test 3: IcebergMetadataGraph is a Pydantic BaseModel subclass with defaults
# ---------------------------------------------------------------------------
def test_iceberg_metadata_graph_is_pydantic_model():
    """IcebergMetadataGraph should be a subclass of pydantic.BaseModel."""
    assert issubclass(IcebergMetadataGraph, BaseModel)


def test_iceberg_metadata_graph_has_empty_list_defaults():
    """IcebergMetadataGraph with required fields should have empty lists for manifest_lists/manifests."""
    graph = IcebergMetadataGraph(
        metadata_s3_key="warehouse/table/metadata/v1.metadata.json",
        metadata={"format-version": 2},
    )
    assert graph.manifest_lists == []
    assert graph.manifests == []


# ---------------------------------------------------------------------------
# Test 4: IcebergMetadataGraph.model_dump() produces a dict (Pydantic v2 API)
# ---------------------------------------------------------------------------
def test_iceberg_metadata_graph_model_dump():
    """IcebergMetadataGraph.model_dump() should return a dict (Pydantic v2 API)."""
    graph = IcebergMetadataGraph(
        metadata_s3_key="warehouse/table/metadata/v1.metadata.json",
        metadata={"format-version": 2},
    )
    dumped = graph.model_dump()
    assert isinstance(dumped, dict)
    assert dumped["metadata_s3_key"] == "warehouse/table/metadata/v1.metadata.json"
    assert dumped["metadata"] == {"format-version": 2}
    assert dumped["manifest_lists"] == []
    assert dumped["manifests"] == []
