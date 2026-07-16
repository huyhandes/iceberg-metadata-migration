# Iceberg Metadata Migration Tool

Python CLI that rewrites hardcoded paths in Apache Iceberg (v2/v3) table metadata and registers the migrated table in AWS Glue Catalog.

Runs as a **post-processing step** after data lands on S3 — no matter how it got there (DataSync, rclone, `aws s3 sync`, HDFS copy, etc.). The tool is **source-agnostic**; it only needs:

1. The Iceberg data already on S3
2. Metadata files containing stale source paths (`s3a://`, `hdfs://`, `/mnt/...`, anything)
3. Those paths rewritten to the S3 location

**Non-destructive:** migrated metadata is written to a `_migrated/` subdirectory under the table location. Originals are never modified, so subsequent sync runs can safely overwrite them.

**Destination:** AWS S3 + AWS Glue Catalog.

## Stack

- **Python >=3.10** — pyiceberg 0.11.x requires it
- **pyiceberg 0.11.1** `[glue,s3fs]` — metadata model, reads v2/v3, `GlueCatalog.register_table()`
- **fastavro 1.12.1** — Avro read/write for manifest list + manifest files (pyiceberg uses it internally; the official `avro` package is buggy on Iceberg schemas — do not use)
- **boto3** — S3 writes + Glue API
- **typer 0.24.1** — CLI
- **pydantic 2.12.5** — validate rewritten metadata before write
- **rich** — CLI output

Dev: **uv** (deps), **pytest** + **moto** (mock S3/Glue), **ruff** (lint/format), **just** (task runner).

## Migration flow

1. Load original with `StaticTable.from_metadata("s3://.../metadata.json")` (read-only — validates + parses).
2. Assert `format-version` is 2 or 3.
3. Rewrite paths: JSON for `metadata.json`, fastavro for the manifest list + manifests.
4. Write back to `_migrated/` via `boto3.put_object` (StaticTable can't write).
5. Register with `GlueCatalog.register_table(identifier, new_metadata_path)`.

Region comes from CLI arg / AWS config — never hardcoded (Glue is region-isolated).

## Commands (`just --list` for all)

```bash
just test              # Unit tests (fast, no Docker)
just check             # lint + types + tests
just lint / typecheck
just seed-all          # Seed all catalog types (Docker)
just test-local        # All local catalog tests
just test-integration  # Full AWS round-trip (needs .env)
just infra-up / infra-down
just tf-apply
```

Copy `.env.example` to `.env`; the justfile auto-loads it via `set dotenv-load`.

## Local dev against MinIO

Set `s3.endpoint` in pyiceberg config to MinIO and `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` to MinIO creds. `moto` mocks Glue; MinIO handles S3.

## Conventions & Architecture

Not yet established — follow existing patterns in the codebase.
