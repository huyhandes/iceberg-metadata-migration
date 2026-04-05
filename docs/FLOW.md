# Migration Flow

Step-by-step pipeline for migrating an Iceberg table's metadata.

## Prerequisites

- Data files already copied to AWS S3 (via DataSync, rclone, aws s3 sync, etc.)
- Metadata files (metadata.json, manifest lists, manifests) also on S3
- Metadata files still contain stale paths from the original source

## Pipeline Steps

### 1. Discovery

**Module:** `discovery/locator.py`, `discovery/compression.py`, `discovery/reader.py`

- Lists objects under `<table_location>/metadata/` in S3
- Finds the latest `vN.metadata.json` (or `vN.gz.metadata.json`) by version number
- Detects compressed metadata via filename extension (`.gz.metadata.json` triggers gzip decompression via `compression.py`)
- Parses the decompressed JSON and loads the current snapshot's manifest list (Avro)
- When loading each Avro file, captures the `avro.codec` from the container header for later round-trip preservation
- For each manifest list entry, loads the referenced manifest file (Avro)
- Returns an `IcebergMetadataGraph` with the current snapshot's files; each Avro file carries its `codec`

### 2. Full Graph Loading

**Module:** `rewrite/graph_loader.py`

- Scans ALL snapshots in metadata.json (not just current)
- Loads any manifest lists and manifests not already in the graph
- Ensures time-travel safety: historical snapshots' paths are also rewritten

### 3. Format-Version Gate

**Module:** `rewrite/engine.py`

Before rewriting begins, `RewriteEngine` checks `format-version` in metadata.json.
Supported versions: 1, 2, 3. Version 4+ is rejected with a clear `ValueError` listing supported versions.
This prevents silent corruption from unknown metadata fields in future Iceberg spec versions.

### 4. Path Rewriting

**Module:** `rewrite/engine.py`, `rewrite/metadata_rewriter.py`, `rewrite/avro_rewriter.py`

Rewrites all path-bearing fields:

| Layer | Fields Rewritten |
|-------|-----------------|
| metadata.json | `location`, `snapshots[*].manifest-list`, `metadata-log[*].metadata-file`, `statistics[*].statistics-path`, `partition-statistics[*].statistics-path` |
| Manifest lists (Avro) | `manifest_path` |
| Manifests (Avro) | `data_file.file_path`, `data_file.referenced_data_file`, `data_file.deletion_vector.path`, `data_file.deletion_vector.file_location` |

After rewriting, all S3 keys are remapped to `_migrated/` paths:
- `warehouse/db/table/metadata/v1.metadata.json` → `warehouse/db/table/_migrated/metadata/v1.metadata.json`

### 5. Validation

**Module:** `validation/validator.py`

Three checks (all must pass):
1. **Structural** — metadata bytes are valid JSON with required fields (`location`, `snapshots`, `current-snapshot-id`)
2. **Residual prefix scan** — zero occurrences of the source prefix in any rewritten file
3. **Manifest count** — manifest list and manifest counts match before and after rewrite

### 6. Write

**Module:** `writer/s3_writer.py`

Writes to `_migrated/` keys in bottom-up dependency order:
1. Manifest files (leaf nodes)
2. Manifest list files (reference manifests)
3. metadata.json (root — written last)

Compressed metadata keys (e.g. `.gz.metadata.json`) are renamed to plain `.metadata.json` in `_migrated/` output. Athena uses file extension to determine encoding, so the extension must match the content (plain JSON).

Avro files are serialized with the same codec as the original (`null`, `deflate`, etc.) via the `codec` field preserved from discovery.

Original files are **never modified**. Skipped entirely in `--dry-run` mode.

### 7. Catalog Registration

**Module:** `catalog/glue_registrar.py`

- Creates or updates a Glue table entry
- Sets Iceberg-specific parameters: `table_type=ICEBERG`, `metadata_location=<_migrated/ URI>`
- Idempotent: if the table already exists, updates `metadata_location` via optimistic locking
- Does NOT create databases — raises an error if the database doesn't exist

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success — all writes + Glue registration completed |
| 1 | Partial failure — some writes succeeded, Glue failed, or mid-write error |
| 2 | Fatal error — failed before any writes (discovery, rewrite, or validation) |
