"""Rewrite path-bearing fields in Avro manifest list and manifest records.

Handles:
  - manifest_path in manifest list records (PATH-04)
  - data_file.file_path in manifest records (PATH-05)

Both functions return deep copies of the records — originals are never modified.
"""

import copy
from typing import Any

from iceberg_migrate.rewrite.config import RewriteConfig


def rewrite_manifest_list_records(
    records: list[dict[str, Any]], config: RewriteConfig
) -> list[dict[str, Any]]:
    """Rewrite manifest_path in manifest list records.

    Returns deep copies of the records so the originals are not modified.

    Args:
        records: List of manifest list Avro records (each has a 'manifest_path' field).
        config: RewriteConfig with src_prefix and dst_prefix.

    Returns:
        New list of records with manifest_path rewritten.
    """
    result = copy.deepcopy(records)
    for record in result:
        path = record.get("manifest_path", "")
        if path:
            record["manifest_path"] = config.replace_prefix(path)
    return result


def rewrite_manifest_records(
    records: list[dict[str, Any]], config: RewriteConfig
) -> list[dict[str, Any]]:
    """Rewrite data_file.file_path in manifest records.

    Returns deep copies of the records so the originals are not modified.

    Args:
        records: List of manifest Avro records (each has a nested 'data_file' dict).
        config: RewriteConfig with src_prefix and dst_prefix.

    Returns:
        New list of records with data_file.file_path rewritten.
    """
    result = copy.deepcopy(records)
    for record in result:
        data_file = record.get("data_file")
        if data_file and isinstance(data_file, dict):
            path = data_file.get("file_path", "")
            if path:
                data_file["file_path"] = config.replace_prefix(path)
            # v3: rewrite referenced_data_file path (position/equality delete manifests, field id 143)
            if "referenced_data_file" in data_file:
                ref = data_file["referenced_data_file"]
                if ref:
                    data_file["referenced_data_file"] = config.replace_prefix(ref)
            # v3: rewrite deletion vector paths (D-09, D-10, D-11)
            dv = data_file.get("deletion_vector")
            if dv and isinstance(dv, dict):
                if "path" in dv:
                    dv["path"] = config.replace_prefix(dv["path"])
                if "file_location" in dv:
                    dv["file_location"] = config.replace_prefix(dv["file_location"])
    return result
