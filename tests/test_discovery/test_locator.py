"""Tests for the metadata locator — numeric version sort and edge cases."""
import pytest

from iceberg_migrate.discovery.locator import _parse_version, find_latest_metadata


BUCKET = "test-bucket"


def _upload(s3_client, keys: list[str]) -> None:
    """Helper: create bucket and upload minimal content for each key."""
    s3_client.create_bucket(Bucket=BUCKET)
    for key in keys:
        s3_client.put_object(Bucket=BUCKET, Key=key, Body=b"{}")


# ---------------------------------------------------------------------------
# Test 1: old-style numeric sort (v1, v2, v10)
# ---------------------------------------------------------------------------
def test_old_style_numeric_sort(s3_client):
    """find_latest_metadata returns v10, NOT v2 (lexicographic loser wins numerically)."""
    prefix = "warehouse/db/table"
    _upload(s3_client, [
        f"{prefix}/metadata/v1.metadata.json",
        f"{prefix}/metadata/v2.metadata.json",
        f"{prefix}/metadata/v10.metadata.json",
    ])
    result = find_latest_metadata(s3_client, BUCKET, prefix)
    assert result == f"{prefix}/metadata/v10.metadata.json"


# ---------------------------------------------------------------------------
# Test 2: new-style numeric sort (00001-, 00010-, 00002-)
# ---------------------------------------------------------------------------
def test_new_style_numeric_sort(s3_client):
    """find_latest_metadata returns 00010-uuid2 (numeric, not lexicographic)."""
    prefix = "warehouse/db/table"
    uuid1 = "abc123"
    uuid2 = "def456"
    uuid3 = "ghi789"
    _upload(s3_client, [
        f"{prefix}/metadata/00001-{uuid1}.metadata.json",
        f"{prefix}/metadata/00010-{uuid2}.metadata.json",
        f"{prefix}/metadata/00002-{uuid3}.metadata.json",
    ])
    result = find_latest_metadata(s3_client, BUCKET, prefix)
    assert result == f"{prefix}/metadata/00010-{uuid2}.metadata.json"


# ---------------------------------------------------------------------------
# Test 3: mixed styles — new-style wins when version is higher
# ---------------------------------------------------------------------------
def test_mixed_styles(s3_client):
    """find_latest_metadata returns 00010-uuid when both styles are present and 10 > 5."""
    prefix = "warehouse/db/table"
    _upload(s3_client, [
        f"{prefix}/metadata/v5.metadata.json",
        f"{prefix}/metadata/00010-uuid-abc.metadata.json",
    ])
    result = find_latest_metadata(s3_client, BUCKET, prefix)
    assert result == f"{prefix}/metadata/00010-uuid-abc.metadata.json"


# ---------------------------------------------------------------------------
# Test 4: empty prefix raises FileNotFoundError
# ---------------------------------------------------------------------------
def test_empty_prefix_raises(s3_client):
    """find_latest_metadata raises FileNotFoundError when no metadata files exist."""
    s3_client.create_bucket(Bucket=BUCKET)
    with pytest.raises(FileNotFoundError) as exc_info:
        find_latest_metadata(s3_client, BUCKET, "empty/prefix")
    assert "empty/prefix" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test 5: single file is returned regardless of version
# ---------------------------------------------------------------------------
def test_single_file_returned(s3_client):
    """find_latest_metadata returns the only available metadata file."""
    prefix = "warehouse/db/table"
    _upload(s3_client, [f"{prefix}/metadata/v3.metadata.json"])
    result = find_latest_metadata(s3_client, BUCKET, prefix)
    assert result == f"{prefix}/metadata/v3.metadata.json"


# ---------------------------------------------------------------------------
# Test 6: _parse_version covers all patterns
# ---------------------------------------------------------------------------
def test_parse_version():
    """_parse_version extracts integer versions from metadata filenames."""
    assert _parse_version("v3.metadata.json") == 3
    assert _parse_version("00047-some-uuid.metadata.json") == 47
    assert _parse_version("random.json") == -1
    assert _parse_version("v10.metadata.json") == 10
    assert _parse_version("00001-uuid.metadata.json") == 1
