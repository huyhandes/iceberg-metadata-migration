"""Extension-based metadata decompression and compression suffix stripping.

Supports compression formats found in real-world Iceberg catalogs:
  .gz.metadata.json   -> gzip (Lakekeeper, Java catalogs with compression enabled)
  .metadata.json      -> plain, no decompression

``strip_compression_suffix`` is used by the engine to ensure output keys never
carry a compression extension — Athena uses the file extension to determine
encoding, so plain JSON must have a plain extension.
"""

from __future__ import annotations

from collections.abc import Callable

import gzip

# Map: compression extension (the part before ".metadata.json") -> decompressor callable
# Only gzip is confirmed in production. Other entries added here as research confirms them.
_COMPRESSION_DISPATCH: dict[str, Callable[[bytes], bytes]] = {
    ".gz": gzip.decompress,
}

_KNOWN_COMPRESSION_SUFFIXES = sorted(_COMPRESSION_DISPATCH.keys())


def decompress_metadata(data: bytes, s3_key: str) -> bytes:
    """Decompress metadata file bytes based on the S3 key extension.

    Dispatches to the appropriate decompressor by examining the filename
    extracted from ``s3_key``. Plain ``.metadata.json`` files are returned
    unchanged. Compressed variants (e.g. ``.gz.metadata.json``) are
    decompressed in memory before returning.

    Args:
        data: Raw bytes of the metadata file as read from S3.
        s3_key: The S3 object key for the metadata file. Only the filename
            component (after the last ``/``) is examined.

    Returns:
        Decompressed (or original, for plain files) bytes ready for JSON
        parsing.

    Raises:
        ValueError: If the filename does not end with ``.metadata.json``
            (with or without a recognised compression prefix), i.e. the file
            is not a metadata file at all.
        gzip.BadGzipFile: If the file claims to be gzip-compressed (via its
            name) but the bytes are not valid gzip data.
    """
    filename = s3_key.split("/")[-1]

    for ext, decompressor in _COMPRESSION_DISPATCH.items():
        if filename.endswith(f"{ext}.metadata.json"):
            return decompressor(data)  # type: ignore[operator]

    if filename.endswith(".metadata.json"):
        # Reject filenames that have an unrecognised compression prefix before
        # .metadata.json, e.g. "v1.xz.metadata.json".  A plain metadata file
        # has a stem that does not start with a dot-separated extension token
        # that looks like a compression suffix (i.e. the part before
        # ".metadata.json" must not itself end with a dotted token that is not
        # a known compression extension).
        stem = filename[: -len(".metadata.json")]  # e.g. "v1" or "v1.xz"
        dot_idx = stem.rfind(".")
        if dot_idx != -1:
            candidate_ext = stem[dot_idx:]  # e.g. ".xz"
            # If this candidate looks like a compression extension (starts with
            # a dot and contains only alphanumeric chars after the dot) but is
            # NOT in our dispatch table, reject it so callers aren't silently
            # handed undecompressed bytes.
            if candidate_ext.lstrip(".").isalnum():
                raise ValueError(
                    f"Cannot determine compression for metadata file {filename!r}. "
                    + f"Supported compression extensions: {_KNOWN_COMPRESSION_SUFFIXES}"
                )
        return data

    raise ValueError(
        f"Cannot determine compression for metadata file {filename!r}. "
        + f"Supported compression extensions: {_KNOWN_COMPRESSION_SUFFIXES}"
    )


def strip_compression_suffix(path: str) -> str:
    """Remove any compression extension before ``.metadata.json`` in a path.

    Ensures output S3 keys always end with plain ``.metadata.json`` regardless
    of the source compression. If the path does not contain a recognised
    compression suffix it is returned unchanged, making this function safe to
    call on any path.

    Args:
        path: An S3 key or filename that may contain a compression extension
            before ``.metadata.json`` (e.g. ``v1.gz.metadata.json``).

    Returns:
        The path with any compression extension removed, so that it ends with
        plain ``.metadata.json``. If no compression extension is present the
        original path is returned unmodified.
    """
    for ext in _COMPRESSION_DISPATCH:
        compressed_suffix = f"{ext}.metadata.json"
        if path.endswith(compressed_suffix):
            return path[: -len(compressed_suffix)] + ".metadata.json"
    return path
