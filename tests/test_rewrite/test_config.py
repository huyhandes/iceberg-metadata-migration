"""Tests for RewriteConfig Pydantic model — PATH-06 s3a normalization."""

import pytest
from pydantic import ValidationError

from iceberg_migrate.rewrite.config import RewriteConfig


SRC = "s3a://src/warehouse"
DST = "s3://dst/warehouse"


# ---------------------------------------------------------------------------
# Test 1: Valid instance with trailing slashes stripped
# ---------------------------------------------------------------------------
def test_rewrite_config_valid_creation():
    """RewriteConfig creates a valid instance and strips trailing slashes."""
    cfg = RewriteConfig(
        src_prefix="s3a://src/warehouse", dst_prefix="s3://dst/warehouse"
    )
    assert cfg.src_prefix == "s3a://src/warehouse"
    assert cfg.dst_prefix == "s3://dst/warehouse"


# ---------------------------------------------------------------------------
# Test 2: Strips trailing slashes from both prefixes
# ---------------------------------------------------------------------------
def test_rewrite_config_strips_trailing_slashes():
    """RewriteConfig strips trailing slashes from both src and dst prefixes."""
    cfg = RewriteConfig(
        src_prefix="s3a://src/warehouse/", dst_prefix="s3://dst/warehouse/"
    )
    assert cfg.src_prefix == "s3a://src/warehouse"
    assert cfg.dst_prefix == "s3://dst/warehouse"


# ---------------------------------------------------------------------------
# Test 3: Invalid scheme raises ValidationError
# ---------------------------------------------------------------------------
def test_rewrite_config_invalid_dst_scheme_raises():
    """RewriteConfig raises ValidationError for non-s3 destination prefix."""
    with pytest.raises(ValidationError):
        RewriteConfig(src_prefix="s3a://src/warehouse", dst_prefix="http://bad")


# ---------------------------------------------------------------------------
# Test 4: Empty string raises ValidationError
# ---------------------------------------------------------------------------
def test_rewrite_config_empty_string_raises():
    """RewriteConfig raises ValidationError for empty prefix strings."""
    with pytest.raises(ValidationError):
        RewriteConfig(src_prefix="", dst_prefix="s3://good")


# ---------------------------------------------------------------------------
# Test 5: replace_prefix replaces s3a:// source prefix with s3:// destination
# ---------------------------------------------------------------------------
def test_replace_prefix_s3a_normalization():
    """replace_prefix returns path with s3a:// src replaced by s3:// dst (PATH-06)."""
    cfg = RewriteConfig(src_prefix=SRC, dst_prefix=DST)
    result = cfg.replace_prefix("s3a://src/warehouse/db/table")
    assert result == "s3://dst/warehouse/db/table"


# ---------------------------------------------------------------------------
# Test 6: replace_prefix returns original if path doesn't match src_prefix
# ---------------------------------------------------------------------------
def test_replace_prefix_unrelated_path_unchanged():
    """replace_prefix returns the original path if it doesn't match src_prefix."""
    cfg = RewriteConfig(src_prefix=SRC, dst_prefix=DST)
    result = cfg.replace_prefix("s3://unrelated/path")
    assert result == "s3://unrelated/path"


# ---------------------------------------------------------------------------
# Test 7: Source prefix allows non-S3 schemes
# ---------------------------------------------------------------------------
def test_config_allows_non_s3_source_prefix():
    """Source prefix can be any scheme — hdfs://, /mnt/, etc."""
    cfg = RewriteConfig(
        src_prefix="hdfs://namenode/warehouse", dst_prefix="s3://bucket/warehouse"
    )
    assert cfg.src_prefix == "hdfs://namenode/warehouse"

    cfg2 = RewriteConfig(
        src_prefix="/mnt/data/warehouse", dst_prefix="s3://bucket/warehouse"
    )
    assert cfg2.src_prefix == "/mnt/data/warehouse"


# ---------------------------------------------------------------------------
# Test 8: Destination prefix still requires S3 scheme
# ---------------------------------------------------------------------------
def test_config_still_requires_s3_dst_prefix():
    """Destination prefix must still be s3:// or s3a://."""
    with pytest.raises(ValidationError):
        RewriteConfig(
            src_prefix="hdfs://namenode/warehouse", dst_prefix="hdfs://other/warehouse"
        )
