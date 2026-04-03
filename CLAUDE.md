<!-- GSD:project-start source:PROJECT.md -->
## Project

**Iceberg Metadata Migration Tool**

A Python CLI tool that rewrites hardcoded paths in Apache Iceberg (v2/v3) table metadata and registers the migrated table in AWS Glue Catalog.

Runs as a **post-processing step** after data has landed on AWS S3 — regardless of how it got there. Works with any data-moving solution: AWS DataSync, rclone, `aws s3 sync/cp`, HDFS copy, or any S3-compatible transfer tool.

**The tool does not care about the source system.** It only requires:
1. The Iceberg table data is already on S3
2. The metadata files contain stale paths from the original source (could be `s3a://`, `hdfs://`, `/mnt/...`, anything)
3. Those paths need rewriting to match the S3 location

**Non-destructive:** Migrated metadata is written to a `_migrated/` subdirectory under the table location. Original metadata files are never modified and can be safely overwritten by subsequent sync runs.

**Destination:** AWS S3 + AWS Glue Catalog.

### Constraints

- **Tech stack**: Python (POC) — fast iteration, pyiceberg ecosystem available
- **Iceberg versions**: Must handle both v2 and v3 metadata formats
- **AWS integration**: Glue Catalog for table registration
- **Data safety**: Non-destructive — original metadata is never overwritten
- **Source agnostic**: Source paths can be any scheme (s3a://, hdfs://, local paths, etc.)
<!-- GSD:project-end -->

<!-- GSD:stack-start source:research/STACK.md -->
## Technology Stack

## Recommended Stack
### Core Technologies
| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| Python | >=3.10 | Runtime | PyIceberg 0.11.x requires Python >=3.10; type hints and match statements improve metadata handling code |
| pyiceberg | 0.11.1 | Iceberg metadata model + Glue catalog | Only first-class Python implementation of Iceberg spec; provides `StaticTable.from_metadata()` to load Avro manifests without running a cluster, `GlueCatalog.register_table()` for direct registration by metadata path; ships its own Avro I/O layer built on fastavro |
| fastavro | 1.12.1 | Avro file read/write for manifest list + manifest files | Fastest Python Avro implementation (C extensions); 8x faster than the official `avro` package; used internally by PyIceberg so version is pinned and compatible; required when reading/writing Avro files outside of PyIceberg's abstractions |
| boto3 | >=1.24.59 (current: 1.42.78) | AWS SDK — S3 + Glue API calls | The AWS SDK for Python; pyiceberg's Glue catalog depends on it; also needed for S3 path validation and any direct `glue.register_table` fallback calls |
| typer | 0.24.1 | CLI interface | Built on Click but uses Python type hints; eliminates boilerplate for argument parsing; produces `--help` automatically; ideal for a focused CLI with 1-3 commands |
### Supporting Libraries
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| s3fs | >=2023.1.0 | S3 filesystem abstraction for PyIceberg FileIO | Required when using `pyiceberg[s3fs]` extra — lets PyIceberg open metadata.json and Avro files from S3 URLs without manual boto3 streaming code |
| pydantic | 2.12.5 | Validate rewritten metadata before write | Use to model the metadata.json structure and assert structural validity after path substitution; catches truncated rewrites or field type drift before the file is written back |
| rich | latest stable | Human-readable CLI output (tables, progress, errors) | Optional but strongly recommended for a migration tool where operators need clear success/failure output and diff-style previews |
### Development Tools
| Tool | Purpose | Notes |
|------|---------|-------|
| uv | Dependency management, virtual environments, Python version pinning | 10-100x faster than pip/poetry for resolution; single binary; handles `pyproject.toml`; recommended for all new Python projects in 2026 |
| pytest | Unit + integration tests | Standard Python test runner; pair with `moto` for mocking S3 and Glue API calls without real AWS access |
| moto | AWS service mocking in tests | Intercepts boto3 calls to Glue and S3; allows full end-to-end testing of register_table and metadata-rewrite paths in CI without AWS credentials |
| ruff | Linting + formatting | Replaces flake8 + isort + black in one fast tool; no configuration needed for a new project |
## Installation
# Create project with uv
# Core runtime dependencies
# Dev dependencies
## Alternatives Considered
| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| pyiceberg (for metadata model + Glue registration) | Raw JSON + fastavro (no pyiceberg) | If you need zero heavyweight dependencies and the metadata schema is known to be stable; POC scope makes this tempting but fragile — pyiceberg validates Iceberg spec compliance automatically |
| pyiceberg GlueCatalog.register_table() | boto3 Glue client.create_table() directly | When you need full control over Glue table DDL or are registering non-standard properties; pyiceberg's catalog layer handles Iceberg-specific metadata serialization for you |
| typer | click | Click is fine if the team has existing click familiarity; typer is strictly better for new tools because type hints remove duplicate documentation |
| uv | poetry | Poetry is reasonable for library publishing workflows; for an internal CLI tool, uv's speed and simplicity wins |
| fastavro | apache-avro (official `avro` package) | Never for this use case — the official package has a known bug parsing some Iceberg manifest schemas ("IgnoredLogicalType: Unknown map, using array") and is 8x slower |
| pydantic | jsonschema | pydantic v2 is faster and produces better error messages; jsonschema is acceptable if the team already uses it |
## What NOT to Use
| Avoid | Why | Use Instead |
|-------|-----|-------------|
| `avro` (apache official PyPI package) | Has documented bugs parsing Iceberg manifest schemas (IgnoredLogicalType errors); 8x slower than fastavro; PyIceberg itself does not use it | `fastavro==1.12.1` |
| PyArrow for Avro reading | PyArrow does not support Avro format; it handles Parquet. A common confusion given PyArrow is a pyiceberg optional dep for Parquet data reading | `fastavro` for Avro, PyArrow only if reading actual data files (Parquet) |
| Spark / PySpark for metadata rewriting | Massively over-engineered for this use case; requires a cluster; 5-minute startup to rewrite files that take milliseconds with direct I/O | `pyiceberg` + `fastavro` directly |
| pyiceberg StaticTable for writes | `StaticTable.from_metadata()` is explicitly read-only — "the static-table does not allow for write operations." Rewritten metadata must be written back via direct S3 put + then registered via GlueCatalog | Direct S3 write (`boto3.put_object`) + `GlueCatalog.register_table()` |
| Hardcoded AWS region strings | Glue catalogs are region-isolated; hardcoding breaks portability across environments | Accept region as a CLI argument or read from AWS config/environment |
## Stack Patterns by Variant
- Use `pyiceberg[glue,s3fs]` so PyIceberg can open S3 URLs natively via FileIO
- Load with `StaticTable.from_metadata("s3://bucket/path/to/metadata.json")` to parse and validate original metadata
- Rewrite paths in the parsed dict (JSON for metadata.json, fastavro for Avro files)
- Write back via `boto3.put_object` to a new metadata path
- Register with `GlueCatalog.register_table(identifier, new_metadata_path)`
- Set `s3.endpoint` in pyiceberg config to point at local MinIO
- Set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` to MinIO credentials
- Use `moto` to mock Glue calls (MinIO itself handles S3 mocking)
- PyIceberg 0.11.x has read support for v3 — no extra handling needed at the library level
- Validate `format-version` field in metadata.json before rewriting; assert it is 2 or 3
## Version Compatibility
| Package | Compatible With | Notes |
|---------|-----------------|-------|
| pyiceberg==0.11.1 | Python >=3.10, <4.0 | Does not support Python 3.9 — critical if running on older Lambda runtimes |
| pyiceberg==0.11.1 | fastavro==1.12.1 | fastavro is pinned in pyiceberg's own dev deps at 1.12.1; use the same version to avoid schema parsing divergence |
| pyiceberg[glue] | boto3>=1.24.59 | boto3 is a loose constraint; using 1.42.x (current) is safe |
| pyiceberg[s3fs] | s3fs>=2023.1.0 | s3fs tracks fsspec; 2023.1.0+ covers all current stable fsspec releases |
| pydantic==2.12.5 | Python >=3.9 | Compatible with Python 3.10+ (our target); v2 API (not v1) |
## Sources
- [pyiceberg PyPI page](https://pypi.org/project/pyiceberg/) — version 0.11.1, release date, Python requirements (HIGH confidence)
- [PyIceberg GitHub releases](https://github.com/apache/iceberg-python/releases) — 0.11.0/0.11.1 changelog, PyArrow bump to 19.0.0 (HIGH confidence)
- [PyIceberg API docs — StaticTable](https://py.iceberg.apache.org/api/) — `StaticTable.from_metadata()` read-only constraint confirmed (HIGH confidence)
- [PyIceberg GlueCatalog reference](https://py.iceberg.apache.org/reference/pyiceberg/catalog/glue/) — `register_table(identifier, metadata_location)` signature (HIGH confidence)
- [PyIceberg configuration docs](https://py.iceberg.apache.org/configuration/) — Glue config keys, S3 FileIO options (HIGH confidence)
- [apache/iceberg-python pyproject.toml](https://github.com/apache/iceberg-python/blob/main/pyproject.toml) — boto3>=1.24.59, s3fs>=2023.1.0, fastavro==1.12.1 exact pins (HIGH confidence)
- [fastavro PyPI](https://pypi.org/project/fastavro/) — version 1.12.1, Python 3.9-3.14 support (HIGH confidence)
- [boto3 PyPI](https://pypi.org/project/boto3/) — current version 1.42.78 (HIGH confidence)
- [typer PyPI](https://pypi.org/project/typer/) — version 0.24.1 (HIGH confidence)
- [pydantic PyPI](https://pypi.org/project/pydantic/) — version 2.12.5 (HIGH confidence)
- [WebSearch: avro package Iceberg manifest bugs](https://github.com/apache/iceberg/issues/6435) — official avro package IgnoredLogicalType errors on manifest schemas (MEDIUM confidence, verified by community issue + PyIceberg's own choice to use fastavro)
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

Conventions not yet established. Will populate as patterns emerge during development.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

Architecture not yet mapped. Follow existing patterns found in the codebase.
<!-- GSD:architecture-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd:quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd:debug` for investigation and bug fixing
- `/gsd:execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd:profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
