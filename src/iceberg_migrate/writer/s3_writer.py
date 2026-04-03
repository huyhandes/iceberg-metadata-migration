"""S3 writer module: writes rewritten Iceberg metadata bytes in bottom-up order.

Write order (D-02):
  1. Manifest files first (bottom of the dependency graph)
  2. Manifest list files second
  3. metadata.json last (top of the dependency graph)

This ordering ensures that if a write fails partway through, lower-level
files already written remain consistent (the metadata.json is only overwritten
once all dependent files are safely in place).

Does NOT catch exceptions — callers are responsible for mapping errors to exit codes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from iceberg_migrate.rewrite.engine import RewriteResult

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


@dataclass
class WriteResult:
    """Counts of objects written to S3 during a migration.

    Used by the output formatter (MigrationSummary) to report results.
    """

    manifests_written: int
    manifest_lists_written: int
    metadata_written: int  # always 1 on success
    metadata_s3_key: str  # the S3 key that was written


def write_all(s3_client: S3Client, bucket: str, result: RewriteResult) -> WriteResult:
    """Write rewritten Iceberg metadata bytes to S3 in bottom-up order.

    Writes in the order: manifests -> manifest lists -> metadata.json.
    This ensures that if any write fails, the already-written lower-level
    files are consistent and the metadata pointer is only updated last.

    Does NOT catch exceptions — any S3 error propagates to the caller.

    Args:
        s3_client: A boto3 S3 client.
        bucket: Target S3 bucket name.
        result: RewriteResult containing bytes to write for each metadata layer.

    Returns:
        WriteResult with counts of objects written.
    """
    # Step 1: Write manifests (bottom of dependency graph)
    for key, data in result.manifest_bytes.items():
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)

    # Step 2: Write manifest lists
    for key, data in result.manifest_list_bytes.items():
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)

    # Step 3: Write metadata.json (top of dependency graph — written last)
    s3_client.put_object(
        Bucket=bucket,
        Key=result.graph.metadata_s3_key,
        Body=result.metadata_bytes,
    )

    return WriteResult(
        manifests_written=len(result.manifest_bytes),
        manifest_lists_written=len(result.manifest_list_bytes),
        metadata_written=1,
        metadata_s3_key=result.graph.metadata_s3_key,
    )
