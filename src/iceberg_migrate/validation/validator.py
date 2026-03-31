"""Pre-write validation gate for rewritten Iceberg metadata.

Three checks (all must pass per D-15):
  VAL-01: Byte-level scan — zero source prefix occurrences in all rewritten bytes
  VAL-02: Structural check — metadata.json is valid JSON with required fields
  VAL-03: Manifest count — counts match before and after rewrite

Usage:
    result = validate_rewrite(original_graph, rewrite_result, src_prefix)
    if not result.passed:
        raise RuntimeError(f"Validation failed: {result.errors}")
"""
from typing import Any

import orjson
from pydantic import BaseModel

from iceberg_migrate.models import IcebergMetadataGraph
from iceberg_migrate.rewrite.engine import RewriteResult


REQUIRED_METADATA_FIELDS = {"location", "snapshots", "current-snapshot-id"}


class ValidationResult(BaseModel):
    """Result of pre-write validation checks.

    passed is True only when all three checks pass (D-15):
      - structural_valid is True
      - residual_prefix_count is 0
      - manifest counts match
    """

    passed: bool
    structural_valid: bool
    residual_prefix_count: int
    manifest_list_count_before: int
    manifest_list_count_after: int
    manifest_count_before: int
    manifest_count_after: int
    errors: list[str] = []


def validate_rewrite(
    original_graph: IcebergMetadataGraph,
    result: RewriteResult,
    src_prefix: str,
) -> ValidationResult:
    """Validate rewritten metadata before writing to S3.

    Three checks (all must pass per D-15):
    1. Structural: metadata_bytes is valid JSON with required fields (VAL-02)
    2. Byte-level: zero source prefix occurrences in all rewritten bytes (VAL-01)
    3. Manifest count: counts match before and after rewrite (VAL-03)

    Args:
        original_graph: The unmodified IcebergMetadataGraph from discovery
                        (must have all snapshots loaded for accurate count comparison).
        result: The RewriteResult from RewriteEngine.rewrite().
        src_prefix: The source prefix to scan for (should be absent from rewritten files).

    Returns:
        ValidationResult with passed=True only if all three checks pass.
    """
    errors: list[str] = []

    # VAL-02: Structural check — must run first because if JSON is invalid,
    # byte-level scan may still succeed while structural check fails.
    structural_valid = _check_structural(result.metadata_bytes, errors)

    # VAL-01: Byte-level scan across all rewritten file bytes
    residual_count = _scan_residual_prefix(src_prefix, result, errors)

    # VAL-03: Manifest count check
    ml_before = len(original_graph.manifest_lists)
    ml_after = len(result.graph.manifest_lists)
    m_before = len(original_graph.manifests)
    m_after = len(result.graph.manifests)
    _check_counts(ml_before, ml_after, m_before, m_after, errors)

    passed = structural_valid and residual_count == 0 and len(errors) == 0

    return ValidationResult(
        passed=passed,
        structural_valid=structural_valid,
        residual_prefix_count=residual_count,
        manifest_list_count_before=ml_before,
        manifest_list_count_after=ml_after,
        manifest_count_before=m_before,
        manifest_count_after=m_after,
        errors=errors,
    )


def _check_structural(metadata_bytes: bytes, errors: list[str]) -> bool:
    """Check that metadata_bytes is valid JSON with all required fields.

    Returns True if structural check passes; appends to errors and returns
    False if not.
    """
    try:
        m: dict[str, Any] = orjson.loads(metadata_bytes)
    except Exception as e:
        errors.append(f"metadata.json is not valid JSON: {e}")
        return False

    missing = REQUIRED_METADATA_FIELDS - set(m.keys())
    if missing:
        errors.append(
            f"Required fields missing from metadata.json: {sorted(missing)}"
        )
        return False

    return True


def _scan_residual_prefix(
    src_prefix: str, result: RewriteResult, errors: list[str]
) -> int:
    """Count occurrences of src_prefix across all rewritten file bytes.

    Scans metadata_bytes + all manifest_list_bytes + all manifest_bytes.
    Appends an error message if any occurrences found.

    Returns total occurrence count (0 on success).
    """
    needle = src_prefix.encode("utf-8")
    all_blobs = (
        [result.metadata_bytes]
        + list(result.manifest_list_bytes.values())
        + list(result.manifest_bytes.values())
    )
    count = sum(blob.count(needle) for blob in all_blobs)
    if count > 0:
        errors.append(
            f"Source prefix {src_prefix!r} still found {count} time(s) in rewritten files"
        )
    return count


def _check_counts(
    ml_before: int,
    ml_after: int,
    m_before: int,
    m_after: int,
    errors: list[str],
) -> None:
    """Check that manifest list and manifest counts match before and after rewrite.

    Appends error messages for any mismatches.
    """
    if ml_before != ml_after:
        errors.append(
            f"Manifest list count mismatch: {ml_before} before, {ml_after} after"
        )
    if m_before != m_after:
        errors.append(
            f"Manifest count mismatch: {m_before} before, {m_after} after"
        )
