"""CLI entry point for iceberg-migrate.

Thin adapter (ADR-0001): builds ``MigrationParams`` from Typer options, calls
``run_migration()``, and maps the returned summary / raised exceptions to exit
codes (0 success / 1 partial / 2 fatal) plus ``rich`` output. All orchestration
lives in ``iceberg_migrate.core``.

Exit codes per D-16:
  0 = success (all S3 writes + Glue registration succeeded)
  1 = partial failure (some writes succeeded, Glue failed, or mid-write exception)
  2 = fatal error (exception before any writes: discovery, rewrite, or validation)
"""

from __future__ import annotations

import sys

import typer

from iceberg_migrate.core import (
    FatalMigrationError,
    MigrationParams,
    MigrationSummary,
    PartialMigrationError,
    run_migration,
)
from iceberg_migrate.output.formatter import render_human, render_json

app = typer.Typer(
    name="iceberg-migrate",
    help="Rewrite Iceberg metadata paths after data migration from any source to AWS S3.",
)


@app.command()
def migrate(
    table_location: str = typer.Option(
        ...,
        "--table-location",
        help="S3 URI of the Iceberg table root (e.g., s3://bucket/warehouse/db/table)",
    ),
    source_prefix: str = typer.Option(
        ...,
        "--source-prefix",
        help="Original path prefix to replace (e.g., s3a://minio-bucket/warehouse)",
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
    glue_database: str | None = typer.Option(
        None,
        "--glue-database",
        help="Glue database name (derived from table_location if omitted)",
    ),
    glue_table: str | None = typer.Option(
        None,
        "--glue-table",
        help="Glue table name (derived from table_location if omitted)",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Emit machine-readable JSON to stdout; human summary goes to stderr",
    ),
    aws_region: str | None = typer.Option(
        None,
        "--aws-region",
        help="AWS region for Glue (falls back to AWS_DEFAULT_REGION env var)",
    ),
) -> None:
    """Rewrite Iceberg metadata paths and register the table in AWS Glue Catalog.

    Runs as a post-processing step after data has landed on AWS S3 (via DataSync,
    rclone, aws s3 sync/cp, or any transfer tool). Creates non-destructive migrated
    metadata under a _migrated/ subdirectory — originals are never modified.
    """
    params = MigrationParams(
        table_location=table_location,
        source_prefix=source_prefix,
        dest_prefix=dest_prefix,
        glue_database=glue_database,
        glue_table=glue_table,
        dry_run=dry_run,
        verbose=verbose,
        aws_region=aws_region,
    )

    try:
        summary = run_migration(params)
    except PartialMigrationError as exc:
        _emit_output(exc.summary, exc.summary.verbose_lines, json_output)
        _echo_error(f"Error: {exc}", json_output)
        raise typer.Exit(code=1) from exc
    except FatalMigrationError as exc:
        _echo_error(f"Fatal error: {exc}", json_output)
        raise typer.Exit(code=2) from exc

    _emit_output(summary, summary.verbose_lines, json_output)
    raise typer.Exit(code=0)


def _echo_error(message: str, json_output: bool) -> None:
    """Echo an error to stderr in JSON mode, stdout otherwise (matches pre-refactor)."""
    typer.echo(message, err=json_output)


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
