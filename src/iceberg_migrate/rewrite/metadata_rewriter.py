"""Rewrite path-bearing fields in Iceberg metadata.json.

Handles all path-bearing field groups per D-05, D-06, D-07:
  - metadata["location"] (PATH-01)
  - metadata["snapshots"][*]["manifest-list"] for ALL snapshots (PATH-02)
  - metadata["metadata-log"][*]["metadata-file"] for ALL entries (PATH-03)
  - metadata["statistics"][*]["statistics-path"] if present
  - metadata["partition-statistics"][*]["statistics-path"] if present
"""
import copy

from iceberg_migrate.rewrite.config import RewriteConfig


def rewrite_metadata_json(metadata: dict, config: RewriteConfig) -> dict:
    """Return a new metadata dict with all path fields rewritten.

    Rewrites (per CONTEXT.md D-05 through D-07):
    - metadata["location"] (PATH-01)
    - metadata["snapshots"][*]["manifest-list"] for ALL snapshots (PATH-02)
    - metadata["metadata-log"][*]["metadata-file"] for ALL entries (PATH-03)
    - metadata["statistics"][*]["statistics-path"] if present
    - metadata["partition-statistics"][*]["statistics-path"] if present

    Returns a deep copy. Original metadata dict is not modified.

    Args:
        metadata: Parsed metadata.json dict.
        config: RewriteConfig with src_prefix and dst_prefix.

    Returns:
        New dict with all path-bearing fields rewritten.
    """
    m = copy.deepcopy(metadata)
    m["location"] = config.replace_prefix(m.get("location", ""))
    for snap in m.get("snapshots", []):
        if "manifest-list" in snap:
            snap["manifest-list"] = config.replace_prefix(snap["manifest-list"])
    for entry in m.get("metadata-log", []):
        if "metadata-file" in entry:
            entry["metadata-file"] = config.replace_prefix(entry["metadata-file"])
    for stat in m.get("statistics", []):
        if "statistics-path" in stat:
            stat["statistics-path"] = config.replace_prefix(stat["statistics-path"])
    for pstat in m.get("partition-statistics", []):
        if "statistics-path" in pstat:
            pstat["statistics-path"] = config.replace_prefix(pstat["statistics-path"])
    return m
