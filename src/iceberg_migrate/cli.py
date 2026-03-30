"""CLI entry point for iceberg-migrate.

Orchestrates the full migration pipeline:
  1. Discovery: load_metadata_graph -> IcebergMetadataGraph
  2. Rewrite: RewriteEngine.rewrite() -> RewriteResult (in-memory path rewriting)
  3. Validation: validate_rewrite() -> ValidationResult (pre-write safety check)
  4. Write: write_all() -> WriteResult (S3 bottom-up write, gated by --dry-run)
  5. Register: register_or_update() -> str (Glue catalog, gated by --dry-run)
  6. Output: render_human or render_json based on --json flag

Exit codes per D-16:
  0 = success (all S3 writes + Glue registration succeeded)
  1 = partial failure (some writes succeeded, Glue failed, or mid-write exception)
  2 = fatal error (exception before any writes: discovery, rewrite, or validation)
"""
from __future__ import annotations

import os
import sys
import time
from typing import Optional

import boto3
import typer

from iceberg_migrate.catalog.glue_registrar import derive_glue_names, register_or_update
from iceberg_migrate.discovery.reader import load_metadata_graph
from iceberg_migrate.output.formatter import (
    MigrationSummary,
    count_rewritten_paths,
    render_human,
    render_json,
)
from iceberg_migrate.rewrite.config import RewriteConfig
from iceberg_migrate.rewrite.engine import RewriteEngine
from iceberg_migrate.s3 import parse_s3_uri
from iceberg_migrate.validation.validator import validate_rewrite


class FatalMigrationError(Exception):
    """Raised when the migration fails before any S3 writes (exit code 2)."""
    pass


class PartialMigrationError(Exception):
    """Raised when the migration fails after some S3 writes have completed (exit code 1).

    Attributes:
        summary: MigrationSummary populated with completed write counts.
    """
    def __init__(self, message: str, summary: MigrationSummary):
        super().__init__(message)
        self.summary = summary


app = typer.Typer(
    name="iceberg-migrate",
    help="Rewrite Iceberg metadata paths after DataSync migration.",
)


@app.command()
def migrate(
    table_location: str = typer.Option(
        ..., "--table-location", help="S3 URI of the Iceberg table root (e.g., s3://bucket/warehouse/db/table)"
    ),
    source_prefix: str = typer.Option(
        ..., "--source-prefix", help="Original path prefix to replace (e.g., s3a://minio-bucket/warehouse)"
    ),
    dest_prefix: str = typer.Option(
        ..., "--dest-prefix", help="New path prefix (e.g., s3://aws-bucket/warehouse)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show planned changes without writing"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", help="Print per-file debug output"
    ),
    glue_database: Optional[str] = typer.Option(
        None, "--glue-database", help="Glue database name (derived from table_location if omitted)"
    ),
    glue_table: Optional[str] = typer.Option(
        None, "--glue-table", help="Glue table name (derived from table_location if omitted)"
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Emit machine-readable JSON to stdout; human summary goes to stderr"
    ),
    aws_region: Optional[str] = typer.Option(
        None, "--aws-region", help="AWS region for Glue (falls back to AWS_DEFAULT_REGION env var)"
    ),
) -> None:
    """Rewrite Iceberg metadata paths from source prefix to destination prefix."""
    start = time.monotonic()

    # --- Setup ---
    bucket, table_key = parse_s3_uri(table_location)
    config = RewriteConfig(src_prefix=source_prefix, dst_prefix=dest_prefix)

    # Derive Glue names from table_location if not provided (D-07)
    glue_db, glue_tbl = derive_glue_names(table_location)
    if glue_database:
        glue_db = glue_database
    if glue_table:
        glue_tbl = glue_table

    # Resolve AWS region (D-07, Pitfall 1)
    region = aws_region or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    s3_client = boto3.client("s3")

    # --- Phase 1: Discovery + Rewrite + Validation (fatal if any step fails, exit 2) ---
    try:
        graph = load_metadata_graph(s3_client, bucket, table_key)
        engine = RewriteEngine(config)
        result = engine.rewrite(graph, s3_client, bucket, table_key)
        validation = validate_rewrite(graph, result, source_prefix)
        if not validation.passed:
            raise FatalMigrationError(
                f"Validation failed before write: {'; '.join(validation.errors)}"
            )
        paths_count, verbose_lines = count_rewritten_paths(result, config)
    except FatalMigrationError as exc:
        if json_output:
            typer.echo(f"Fatal error: {exc}", err=True)
        else:
            typer.echo(f"Fatal error: {exc}", err=False)
        raise typer.Exit(code=2)
    except Exception as exc:
        if json_output:
            typer.echo(f"Fatal error: {exc}", err=True)
        else:
            typer.echo(f"Fatal error: {exc}", err=False)
        raise typer.Exit(code=2)

    # --- Phase 2: Write + Register (partial failure = exit 1, fatal before writes = exit 2) ---
    writes_completed = 0
    metadata_s3_key = result.graph.metadata_s3_key
    metadata_s3_uri = f"s3://{bucket}/{metadata_s3_key}"
    glue_action: Optional[str] = None
    write_counts = {"manifests": 0, "manifest_lists": 0, "metadata": 0}

    if not dry_run:
        try:
            from iceberg_migrate.writer.s3_writer import write_all
            write_result = write_all(s3_client, bucket, result)
            writes_completed = (
                write_result.manifests_written
                + write_result.manifest_lists_written
                + write_result.metadata_written
            )
            write_counts = {
                "manifests": write_result.manifests_written,
                "manifest_lists": write_result.manifest_lists_written,
                "metadata": write_result.metadata_written,
            }

            # Register in Glue Catalog
            from pyiceberg.catalog.glue import GlueCatalog
            glue_client = boto3.client("glue", region_name=region)
            catalog = GlueCatalog("glue", **{"glue.region": region})
            glue_action = register_or_update(
                catalog, glue_client, glue_db, glue_tbl, metadata_s3_uri
            )
        except Exception as exc:
            # Determine partial vs. fatal based on how many writes completed (D-16, Pitfall 4)
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
                status="partial_failure" if writes_completed > 0 else "fatal_error",
            )
            _emit_output(summary, verbose_lines if verbose else None, json_output)
            if json_output:
                typer.echo(f"Error: {exc}", err=True)
            else:
                typer.echo(f"Error: {exc}")
            if writes_completed > 0:
                raise typer.Exit(code=1)
            else:
                raise typer.Exit(code=2)

    # --- Phase 3: Build summary and emit output ---
    duration = time.monotonic() - start

    if dry_run:
        # In dry-run, counts represent what would be written
        summary = MigrationSummary(
            source_prefix=source_prefix,
            dest_prefix=dest_prefix,
            table_location=table_location,
            manifests_written=len(result.manifest_bytes),
            manifest_lists_written=len(result.manifest_list_bytes),
            metadata_written=1,
            paths_rewritten=paths_count,
            glue_database=glue_db,
            glue_table=glue_tbl,
            glue_action=None,
            metadata_s3_key=metadata_s3_key,
            duration_seconds=duration,
            dry_run=True,
            status="success",
        )
    else:
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
            duration_seconds=duration,
            dry_run=False,
            status="success",
        )

    _emit_output(summary, verbose_lines if verbose else None, json_output)
    raise typer.Exit(code=0)


def _emit_output(
    summary: MigrationSummary,
    verbose_lines: list[str] | None,
    json_output: bool,
) -> None:
    """Emit migration output to the appropriate streams.

    In JSON mode (D-13, Pitfall 5):
      - JSON blob goes to stdout (pipeable)
      - Human summary goes to stderr

    In human mode (default):
      - Human summary goes to stdout
    """
    if json_output:
        # JSON to stdout; human summary to stderr to avoid contaminating JSON stream
        print(render_json(summary))
        render_human(summary, verbose_lines, file=sys.stderr)
    else:
        render_human(summary, verbose_lines)


if __name__ == "__main__":
    app()
