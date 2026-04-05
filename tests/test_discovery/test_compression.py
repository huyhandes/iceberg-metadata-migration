"""Tests for extension-based metadata decompression and suffix stripping."""

import gzip

import pytest

from iceberg_migrate.discovery.compression import (
    decompress_metadata,
    strip_compression_suffix,
)


# ---------------------------------------------------------------------------
# decompress_metadata tests
# ---------------------------------------------------------------------------


def test_decompress_metadata_plain_passthrough():
    """Plain .metadata.json data is returned unchanged."""
    data = b'{"format-version": 2}'
    result = decompress_metadata(data, "warehouse/db/table/metadata/v1.metadata.json")
    assert result == data


def test_decompress_metadata_gzip():
    """.gz.metadata.json data is gzip-decompressed before returning."""
    original = b'{"format-version": 2, "table-uuid": "abc"}'
    compressed = gzip.compress(original)
    result = decompress_metadata(
        compressed, "warehouse/db/table/metadata/v1.gz.metadata.json"
    )
    assert result == original


def test_decompress_metadata_unknown_extension_raises():
    """An unrecognised compression extension raises ValueError with helpful message."""
    with pytest.raises(ValueError, match="Supported compression extensions"):
        decompress_metadata(
            b"some bytes", "warehouse/db/table/metadata/v1.xz.metadata.json"
        )


def test_decompress_metadata_non_metadata_file_raises():
    """A non-metadata file (e.g. .avro) raises ValueError."""
    with pytest.raises(ValueError, match="Supported compression extensions"):
        decompress_metadata(
            b"avro bytes", "warehouse/db/table/metadata/manifest-1.avro"
        )


# ---------------------------------------------------------------------------
# strip_compression_suffix tests
# ---------------------------------------------------------------------------


def test_strip_compression_suffix_gz():
    """v1.gz.metadata.json -> v1.metadata.json"""
    assert strip_compression_suffix("v1.gz.metadata.json") == "v1.metadata.json"


def test_strip_compression_suffix_plain_unchanged():
    """v1.metadata.json is returned unchanged — no compression suffix present."""
    assert strip_compression_suffix("v1.metadata.json") == "v1.metadata.json"


def test_strip_compression_suffix_full_path():
    """Full S3 key path: compression suffix stripped, directory preserved."""
    input_path = "warehouse/db/table/metadata/v1.gz.metadata.json"
    expected = "warehouse/db/table/metadata/v1.metadata.json"
    assert strip_compression_suffix(input_path) == expected


def test_strip_compression_suffix_avro_unchanged():
    """.avro file has no .metadata.json suffix — returned unmodified."""
    path = "warehouse/db/table/metadata/manifest-1.avro"
    assert strip_compression_suffix(path) == path
