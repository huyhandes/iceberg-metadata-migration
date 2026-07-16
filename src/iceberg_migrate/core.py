"""Shared migration core (ADR-0001).

The single copy of the six-step migration orchestration:
  1. Discovery: load_metadata_graph -> IcebergMetadataGraph
  2. Rewrite: RewriteEngine.rewrite() -> RewriteResult (in-memory path rewriting)
  3. Validation: validate_rewrite() -> ValidationResult (pre-write safety check)
  4. Write: write_all() -> WriteResult (S3 bottom-up write, gated by --dry-run)
  5. Register: register_or_update() -> str (Glue catalog, gated by --dry-run)
  6. Build MigrationSummary

Both the Typer CLI and the Lambda handler call ``run_migration(params)``.

Failure model (ADR-0002):
  - On success: returns a populated ``MigrationSummary``.
  - On failure BEFORE any S3 write: raises ``FatalMigrationError``.
  - On failure AFTER some writes completed: raises ``PartialMigrationError``
    carrying the partially-populated ``MigrationSummary`` on ``.summary``.

This module has no typer / rich / sys.exit dependencies.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass

import boto3

from iceberg_migrate.catalog.glue_registrar import derive_glue_names, register_or_update
from iceberg_migrate.discovery.reader import load_metadata_graph
from iceberg_migrate.output.formatter import (
    MigrationSummary,
    count_rewritten_paths,
)
from iceberg_migrate.rewrite.config import RewriteConfig
from iceberg_migrate.rewrite.engine import RewriteEngine
from iceberg_migrate.s3 import parse_s3_uri
from iceberg_migrate.validation.validator import validate_rewrite

__all__ = [
    "FatalMigrationError",
    "MigrationParams",
    "MigrationSummary",
    "PartialMigrationError",
    "run_migration",
]


@dataclass(frozen=True)
class MigrationParams:
    """Confirmed inputs for ``run_migration`` (ADR-0001, issue #1).

    Built by the CLI / Lambda handler; carries exactly the confirmed fields.
    Required: ``table_location``, ``source_prefix``, ``dest_prefix``.
    Optional: ``glue_database``, ``glue_table``, ``dry_run``, ``verbose``,
    ``aws_region`` (same defaults as the CLI).
    """

    table_location: str
    source_prefix: str
    dest_prefix: str
    glue_database: str | None = None
    glue_table: str | None = None
    dry_run: bool = False
    verbose: bool = False
    aws_region: str | None = None


class FatalMigrationError(Exception):
    """Raised when migration fails before any S3 writes (CLI exit code 2)."""

    pass


class PartialMigrationError(Exception):
    """Raised when migration fails after some S3 writes completed (CLI exit 1).

    Attributes:
        summary: MigrationSummary populated with completed write counts.
    """

    summary: MigrationSummary

    def __init__(self, message: str, summary: MigrationSummary):
        super().__init__(message)
        self.summary = summary


def run_migration(params: MigrationParams) -> MigrationSummary:
    """Run the full 6-step migration; return a summary or raise.

    Region resolution, Glue-name derivation, and boto3 client creation all
    happen here (matching the original CLI). On success returns a populated
    ``MigrationSummary`` with ``verbose_lines`` stashed on the summary when
    ``params.verbose`` is set. On failure raises ``FatalMigrationError``
    (no writes done) or ``PartialMigrationError`` (writes completed > 0).
    """
    start = time.monotonic()

    table_location = params.table_location
    source_prefix = params.source_prefix
    dest_prefix = params.dest_prefix

    # --- Setup ---
    bucket, table_key = parse_s3_uri(table_location)
    config = RewriteConfig(src_prefix=source_prefix, dst_prefix=dest_prefix)

    # Derive Glue names from table_location if not provided (D-07)
    glue_db, glue_tbl = derive_glue_names(table_location)
    if params.glue_database:
        glue_db = params.glue_database
    if params.glue_table:
        glue_tbl = params.glue_table

    # Resolve AWS region (D-07, Pitfall 1)
    region = params.aws_region or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    s3_client = boto3.client("s3")

    # --- Phase 1: Discovery + Rewrite + Validation (fatal if any step fails) ---
    # Any exception before writes is fatal (ADR-0002): wrap so generic errors
    # surface as FatalMigrationError, matching the original CLI's exit-2 path.
    try:
        graph = load_metadata_graph(s3_client, bucket, table_key)
        engine = RewriteEngine(config)
        result = engine.rewrite(graph, s3_client, bucket, table_key)
        validation = validate_rewrite(result.graph, result, source_prefix)
        if not validation.passed:
            raise FatalMigrationError(
                f"Validation failed before write: {'; '.join(validation.errors)}"
            )
        paths_count, verbose_lines = count_rewritten_paths(result, config)
    except FatalMigrationError:
        raise
    except Exception as exc:
        raise FatalMigrationError(str(exc)) from exc

    # --- Phase 2: Write + Register (partial failure if writes completed > 0) ---
    metadata_s3_key = result.graph.metadata_s3_key
    metadata_s3_uri = f"s3://{bucket}/{metadata_s3_key}"
    glue_action: str | None = None
    verbose = verbose_lines if params.verbose else None

    if params.dry_run:
        # Dry-run: counts represent what would be written.
        write_counts = {
            "manifests": len(result.manifest_bytes),
            "manifest_lists": len(result.manifest_list_bytes),
            "metadata": 1,
        }
    else:
        write_counts = {"manifests": 0, "manifest_lists": 0, "metadata": 0}
        try:
            from iceberg_migrate.writer.s3_writer import write_all

            write_result = write_all(s3_client, bucket, result)
            write_counts = {
                "manifests": write_result.manifests_written,
                "manifest_lists": write_result.manifest_lists_written,
                "metadata": write_result.metadata_written,
            }

            # Register in Glue Catalog
            glue_client = boto3.client("glue", region_name=region)
            glue_action = register_or_update(
                None,
                glue_client,
                glue_db,
                glue_tbl,
                metadata_s3_uri,
                metadata=result.graph.metadata,
            )
        except Exception as exc:
            # Partial vs. fatal by writes completed (D-16, Pitfall 4).
            writes_completed = sum(write_counts.values())
            status = "partial_failure" if writes_completed > 0 else "fatal_error"
            summary = MigrationSummary(
                source_prefix=source_prefix,
                dest_prefix=dest_prefix,
                table_location=table_location,
                manifests_written=write_counts["manifests"],
                manifest_lists_written=write_counts["manifest_lists"],
                metadata_written=write_counts["metadata"],
                paths_rewritten=paths_count,
                glue_database=glue_db,
                glue_table=glue_tbl,
                glue_action=glue_action,
                metadata_s3_key=metadata_s3_key,
                duration_seconds=time.monotonic() - start,
                dry_run=False,
                status=status,
                verbose_lines=verbose,
            )
            if writes_completed > 0:
                raise PartialMigrationError(str(exc), summary) from exc
            raise FatalMigrationError(str(exc)) from exc

    # --- Phase 3: Build success summary ---
    return MigrationSummary(
        source_prefix=source_prefix,
        dest_prefix=dest_prefix,
        table_location=table_location,
        manifests_written=write_counts["manifests"],
        manifest_lists_written=write_counts["manifest_lists"],
        metadata_written=write_counts["metadata"],
        paths_rewritten=paths_count,
        glue_database=glue_db,
        glue_table=glue_tbl,
        glue_action=glue_action,
        metadata_s3_key=metadata_s3_key,
        duration_seconds=time.monotonic() - start,
        dry_run=params.dry_run,
        status="success",
        verbose_lines=verbose,
    )
