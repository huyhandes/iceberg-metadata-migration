"""Output formatter for the Iceberg migration tool.

Provides:
  - MigrationSummary: dataclass capturing all counts for human and JSON output
  - render_human: structured rich terminal output per D-12, D-14, D-15
  - render_json: machine-readable JSON to stdout per D-13
  - count_rewritten_paths: counts total path substitutions across all rewritten bytes

Output routing per D-13:
  - Human summary: goes to stdout (or stderr when --json flag active)
  - JSON output: always to stdout (pipeable)
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import IO, TYPE_CHECKING

import orjson
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

if TYPE_CHECKING:
    from iceberg_migrate.rewrite.config import RewriteConfig
    from iceberg_migrate.rewrite.engine import RewriteResult


@dataclass
class MigrationSummary:
    """All counts and metadata needed for human-readable and JSON output.

    Populated from WriteResult counts after a successful migration.
    Used by both render_human and render_json.
    """
    source_prefix: str
    dest_prefix: str
    table_location: str
    manifests_written: int
    manifest_lists_written: int
    metadata_written: int
    paths_rewritten: int  # total path substitutions across all files
    glue_database: str | None = None
    glue_table: str | None = None
    glue_action: str | None = None  # "created", "updated", or None
    metadata_s3_key: str = ""
    duration_seconds: float = 0.0
    dry_run: bool = False
    status: str = "success"  # "success", "partial_failure", "fatal_error"


def render_human(
    summary: MigrationSummary,
    verbose_lines: list[str] | None = None,
    file: IO[str] | None = None,
) -> None:
    """Render a structured human-readable migration summary using rich.

    In dry_run mode, the panel header notes "Dry run" and counts use
    "would be rewritten" labels per D-14.

    If verbose_lines is provided, they are printed before the summary per D-15.

    Args:
        summary: MigrationSummary with all counts and metadata.
        verbose_lines: Optional per-file substitution counts for --verbose mode.
        file: Output file (defaults to sys.stdout; use sys.stderr when in JSON mode).
    """
    if file is None:
        file = sys.stdout

    console = Console(file=file)

    # Verbose: print per-file substitution lines before the summary (D-15)
    if verbose_lines:
        console.print("[dim]Verbose output:[/dim]")
        for line in verbose_lines:
            console.print(f"[dim]{line}[/dim]")

    # Build summary table
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column("Field", style="bold cyan")
    table.add_column("Value")

    table.add_row("Source prefix", summary.source_prefix)
    table.add_row("Destination prefix", summary.dest_prefix)
    table.add_row("Table location", summary.table_location)
    table.add_row("Metadata file", summary.metadata_s3_key or "(none)")

    # Use "would be" labels in dry_run mode (D-14)
    if summary.dry_run:
        table.add_row("Manifests", f"{summary.manifests_written} would be written")
        table.add_row("Manifest lists", f"{summary.manifest_lists_written} would be written")
        table.add_row("Paths", f"{summary.paths_rewritten} would be rewritten")
    else:
        table.add_row("Manifests written", str(summary.manifests_written))
        table.add_row("Manifest lists written", str(summary.manifest_lists_written))
        table.add_row("Paths rewritten", str(summary.paths_rewritten))

    if summary.glue_database and summary.glue_table:
        glue_status = f"{summary.glue_database}.{summary.glue_table}"
        if summary.glue_action:
            glue_status += f" ({summary.glue_action})"
        table.add_row("Glue table", glue_status)

    table.add_row("Duration", f"{summary.duration_seconds:.2f}s")

    # Panel title reflects dry_run and status
    if summary.dry_run:
        title = "[yellow]Dry run -- no files written[/yellow]"
        border_style = "yellow"
    elif summary.status == "success":
        title = "[green]Migration complete[/green]"
        border_style = "green"
    elif summary.status == "partial_failure":
        title = "[yellow]Migration partial failure[/yellow]"
        border_style = "yellow"
    else:
        title = "[red]Migration failed[/red]"
        border_style = "red"

    panel = Panel(table, title=title, border_style=border_style)
    console.print(panel)


def render_json(summary: MigrationSummary) -> str:
    """Return a JSON string representation of the migration summary.

    Output is intended for stdout only — human summary should be directed
    to stderr when JSON mode is active (per D-13: human summary to stderr,
    JSON to stdout for piping).

    Uses orjson.dumps for consistency with project patterns.

    Args:
        summary: MigrationSummary with all counts and metadata.

    Returns:
        JSON-encoded string of the migration summary.
    """
    data = {
        "status": summary.status,
        "dry_run": summary.dry_run,
        "source_prefix": summary.source_prefix,
        "dest_prefix": summary.dest_prefix,
        "table_location": summary.table_location,
        "metadata_s3_key": summary.metadata_s3_key,
        "counts": {
            "manifests_written": summary.manifests_written,
            "manifest_lists_written": summary.manifest_lists_written,
            "metadata_written": summary.metadata_written,
            "paths_rewritten": summary.paths_rewritten,
        },
        "glue": {
            "database": summary.glue_database,
            "table": summary.glue_table,
            "action": summary.glue_action,
        },
        "duration_seconds": summary.duration_seconds,
    }
    return orjson.dumps(data, option=orjson.OPT_INDENT_2).decode("utf-8")


def count_rewritten_paths(
    result: RewriteResult,
    config: RewriteConfig,
) -> tuple[int, list[str]]:
    """Count total path substitutions across all rewritten metadata bytes.

    Scans the raw bytes of all rewritten files (metadata_bytes,
    manifest_list_bytes, manifest_bytes) for occurrences of the destination
    prefix. Each occurrence represents a successfully rewritten path.
    This is a byte-level count, so it covers all rewritten layers
    including JSON and Avro files.

    Args:
        result: RewriteResult containing all rewritten bytes.
        config: RewriteConfig with dst_prefix used to count rewritten paths.

    Returns:
        Tuple of (total_count, verbose_lines) where:
          - total_count: total number of path substitutions across all files
          - verbose_lines: per-file substitution counts formatted as
            "  {filename}  : {N} paths rewritten"
    """
    dst_bytes = config.dst_prefix.encode("utf-8")
    total_count = 0
    verbose_lines: list[str] = []

    def _count_file(label: str, data: bytes) -> int:
        n = data.count(dst_bytes)
        if n > 0:
            verbose_lines.append(f"  {label}  : {n} paths rewritten")
        return n

    # Count in metadata.json bytes
    if result.metadata_bytes:
        filename = result.graph.metadata_s3_key.split("/")[-1]
        total_count += _count_file(filename, result.metadata_bytes)

    # Count in manifest list bytes
    for key, data in result.manifest_list_bytes.items():
        filename = key.split("/")[-1]
        total_count += _count_file(filename, data)

    # Count in manifest bytes
    for key, data in result.manifest_bytes.items():
        filename = key.split("/")[-1]
        total_count += _count_file(filename, data)

    return total_count, verbose_lines
