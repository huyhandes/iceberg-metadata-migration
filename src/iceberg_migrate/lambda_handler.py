"""AWS Lambda entry point for an Iceberg metadata migration (issues #1, #4).

Thin adapter over :func:`iceberg_migrate.core.run_migration`:

- Maps the Lambda ``event`` (snake_case, 1:1 with CLI args) to a
  :class:`~iceberg_migrate.core.MigrationParams`.
- On success returns a plain ``dict`` whose shape matches
  :func:`iceberg_migrate.output.formatter.render_json` (Lambda serializes the
  dict to JSON for the caller).
- On any failure **re-raises**, so Lambda reports a ``FunctionError`` and the
  orchestrator (Airflow) marks the task failed and retries (ADR-0002).

Uses stdlib :mod:`logging` (CloudWatch-friendly, no ``rich`` / ANSI escapes).
"""

from __future__ import annotations

import logging
from typing import Any

from iceberg_migrate.core import MigrationParams, MigrationSummary, run_migration

logger = logging.getLogger(__name__)

# Required event keys (issue #1 user story 3); missing any -> raise before work.
_REQUIRED = ("table_location", "source_prefix", "dest_prefix")


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """Run a migration for a Lambda invocation.

    Args:
        event: JSON object mirroring CLI args (snake_case). Required keys:
            ``table_location``, ``source_prefix``, ``dest_prefix``. Optional:
            ``glue_database``, ``glue_table``, ``dry_run`` (default False),
            ``verbose``, ``aws_region`` (falls back to core's env resolution).
        _context: Lambda runtime context (unused; accepted per Lambda contract).

    Returns:
        Dict matching :func:`render_json`'s shape on success.

    Raises:
        KeyError: if a required event key is missing (before any work).
        FatalMigrationError / PartialMigrationError: propagated from
            :func:`run_migration` (ADR-0002).
    """
    # Validate required keys BEFORE building params or any S3/Glue work (ADR-0002).
    missing = [k for k in _REQUIRED if not event.get(k)]
    if missing:
        raise KeyError(f"missing required event key(s): {', '.join(missing)}")

    table_location = event["table_location"]
    logger.info("migration handler start table_location=%s", table_location)

    params = MigrationParams(
        table_location=table_location,
        source_prefix=event["source_prefix"],
        dest_prefix=event["dest_prefix"],
        glue_database=event.get("glue_database"),
        glue_table=event.get("glue_table"),
        dry_run=bool(event.get("dry_run", False)),
        verbose=bool(event.get("verbose", False)),
        aws_region=event.get("aws_region"),  # None -> core resolves env.
    )

    try:
        summary = run_migration(params)
    except Exception:
        logger.exception("migration failed table_location=%s", table_location)
        raise

    logger.info(
        "migration success table_location=%s status=%s dry_run=%s",
        table_location,
        summary.status,
        summary.dry_run,
    )
    return _to_dict(summary)


def _to_dict(summary: MigrationSummary) -> dict[str, Any]:
    """Map a MigrationSummary to the render_json dict shape (issue #1 US-5)."""
    return {
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
