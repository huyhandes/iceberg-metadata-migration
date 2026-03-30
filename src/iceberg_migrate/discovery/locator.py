"""Metadata locator: find the highest-versioned metadata.json in S3.

Supports both naming conventions used by Iceberg writers:
  - Old-style: v1.metadata.json, v2.metadata.json, v10.metadata.json
  - New-style: 00001-uuid.metadata.json, 00010-uuid.metadata.json
"""
import re


def _parse_version(filename: str) -> int:
    """Extract the numeric version from a metadata filename.

    Args:
        filename: The bare filename (no directory path).

    Returns:
        The integer version extracted from the filename.
        Returns -1 if the filename matches neither known pattern.
    """
    # New-style: leading zero-padded integer followed by a dash
    # e.g. "00047-some-uuid.metadata.json" -> 47
    new_style = re.match(r'^(\d+)-', filename)
    if new_style:
        return int(new_style.group(1))

    # Old-style: v<N>.metadata.json
    # e.g. "v3.metadata.json" -> 3
    old_style = re.match(r'^v(\d+)\.metadata\.json$', filename)
    if old_style:
        return int(old_style.group(1))

    return -1


def find_latest_metadata(s3_client, bucket: str, table_prefix: str) -> str:
    """Return the S3 key of the highest-versioned metadata.json in *bucket*.

    Scans ``<table_prefix>/metadata/`` and selects the file with the largest
    version number, using ``_parse_version`` for numeric (not lexicographic)
    comparison so that v10 > v9 even though "v10" < "v9" lexicographically.

    Args:
        s3_client: A boto3 S3 client (real or moto-backed).
        bucket: S3 bucket name.
        table_prefix: Prefix for the Iceberg table (no trailing slash needed).

    Returns:
        The S3 key string for the latest metadata.json file.

    Raises:
        FileNotFoundError: If no .metadata.json files exist under the prefix.
    """
    metadata_prefix = table_prefix.rstrip("/") + "/metadata/"

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=metadata_prefix)

    candidates: list[tuple[str, int]] = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.split("/")[-1]
            if filename.endswith(".metadata.json"):
                version = _parse_version(filename)
                candidates.append((key, version))

    if not candidates:
        raise FileNotFoundError(
            f"No metadata.json files found under s3://{bucket}/{metadata_prefix}"
        )

    latest_key, _ = max(candidates, key=lambda x: x[1])
    return latest_key
