"""S3 utility functions for URI parsing and object access."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Parse s3:// or s3a:// URI into (bucket, key).

    Args:
        uri: An S3 URI like s3://bucket/path/to/key or s3a://bucket/path/to/key

    Returns:
        Tuple of (bucket_name, object_key)

    Raises:
        ValueError: If the URI scheme is not s3 or s3a
    """
    parsed = urlparse(uri)
    if parsed.scheme not in ("s3", "s3a"):
        raise ValueError(f"Expected s3:// or s3a:// URI, got: {uri!r}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket:
        raise ValueError(f"Missing bucket in URI: {uri!r}")
    return bucket, key


def get_s3_object_bytes(s3_client: S3Client, bucket: str, key: str) -> bytes:
    """Download an S3 object and return its contents as bytes.

    Args:
        s3_client: A boto3 S3 client
        bucket: S3 bucket name
        key: S3 object key

    Returns:
        The object's contents as bytes

    Raises:
        FileNotFoundError: If the object does not exist
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    except s3_client.exceptions.NoSuchKey:
        raise FileNotFoundError(f"s3://{bucket}/{key} not found")
