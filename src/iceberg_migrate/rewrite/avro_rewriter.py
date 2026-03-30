"""Rewrite path-bearing fields in Avro manifest list and manifest records.

Handles:
  - manifest_path in manifest list records (PATH-04)
  - data_file.file_path in manifest records (PATH-05)

Both functions return deep copies of the records — originals are never modified.
"""
import copy

from iceberg_migrate.rewrite.config import RewriteConfig


def rewrite_manifest_list_records(records: list[dict], config: RewriteConfig) -> list[dict]:
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


def rewrite_manifest_records(records: list[dict], config: RewriteConfig) -> list[dict]:
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
    return result
