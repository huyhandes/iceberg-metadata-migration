# iceberg-migrate

**Rewrite Apache Iceberg metadata paths after a data migration — without a JVM.**

A pure-Python CLI that fixes stale paths in Iceberg table metadata after data has landed on AWS S3. Runs in seconds, not minutes. No Spark cluster. No JVM. Drop it into any post-sync pipeline.

---

## The Problem

You move Iceberg table data to AWS S3 — via DataSync, rclone, `aws s3 sync`, or any other tool. The data files are there. But the metadata still contains the old source paths:

```
s3a://on-prem-minio/warehouse/db/events  ← stale
hdfs://namenode:8020/warehouse/db/events ← stale
/mnt/nas/iceberg/warehouse/db/events     ← stale
```

Athena, Glue, and EMR refuse to read the table. The metadata needs updating.

## The Solution

```bash
iceberg-migrate migrate \
  --table-location  s3://my-bucket/warehouse/db/events \
  --source-prefix   s3a://on-prem-minio/warehouse \
  --dest-prefix     s3://my-bucket/warehouse
```

That's it. The tool reads the Iceberg metadata tree, rewrites every path-bearing field, writes migrated copies to `_migrated/`, and registers the table in AWS Glue Catalog — all without touching the original files.

---

## Why Not Just Use Spark?

| | iceberg-migrate | Spark job |
|---|---|---|
| Runtime | **~3 seconds** | ~5 minutes (cluster warmup) |
| Dependencies | **Python + boto3** | JVM, Spark, cluster or serverless |
| Infrastructure | **None** | EMR, Glue ETL, or local Spark |
| Runs in CI/CD | **Yes** | Painful |
| Source path format | **Any** (`s3a://`, `hdfs://`, local) | S3 only (typically) |

The tool rewrites *metadata files* — JSON and Avro, typically a few kilobytes. There is no reason to spin up a cluster for this.

---

## Verified Query Engines

Migrated tables have been end-to-end tested against three AWS query engines:

| Engine | Test type | Result |
|--------|-----------|--------|
| **Amazon Athena** | SQL queries via Athena API | ✅ 7 tests passing |
| **AWS Glue ETL 5.1** | PySpark jobs via Glue Data Catalog | ✅ 3 tests passing |
| **EMR Serverless 7.12.0** | PySpark jobs via Iceberg Glue catalog | ✅ 3 tests passing |

Every migrated table is readable for `SELECT`, `JOIN`, and cross-table joins.

---

## Source Catalog Compatibility

The tool is source-agnostic. It handles whatever the source catalog wrote:

| Source | Metadata format | Avro codec | Status |
|--------|----------------|------------|--------|
| Lakekeeper (Rust) | Gzip-compressed `.gz.metadata.json` | `deflate` | ✅ |
| PyIceberg SQL / SQLite | Plain JSON | `deflate` | ✅ |
| Hive Metastore (HMS) | Plain JSON | `deflate` | ✅ |
| Java catalogs (Glue, Nessie, Polaris, etc.) | Plain JSON | `deflate` | ✅ |
| Hadoop filesystem | Plain JSON | any | ✅ |

Compression is detected from the filename extension, not file magic bytes. Avro codec is preserved exactly on round-trip — the tool never silently recompress or decompress Avro manifests.

---

## Iceberg Version Support

| Format version | Supported | Notes |
|----------------|-----------|-------|
| v1 | ✅ | Original format |
| v2 | ✅ | Row-level deletes (position + equality) |
| v3 | ✅ | Deletion vectors, `referenced_data_file` |
| v4+ | ❌ | Rejected with a clear error — prevents silent corruption |

---

## Installation

```bash
# From source (uv)
uv sync

# Or pip
pip install .
```

**Runtime requirements:** Python ≥ 3.10, AWS credentials with S3 + Glue access.

---

## Usage

### Migrate a table

```bash
iceberg-migrate migrate \
  --table-location  s3://my-bucket/warehouse/db/events \
  --source-prefix   s3a://old-minio/warehouse \
  --dest-prefix     s3://my-bucket/warehouse
```

### Dry run first

```bash
iceberg-migrate migrate \
  --table-location  s3://my-bucket/warehouse/db/events \
  --source-prefix   s3a://old-minio/warehouse \
  --dest-prefix     s3://my-bucket/warehouse \
  --dry-run --verbose
```

### HDFS source

```bash
iceberg-migrate migrate \
  --table-location  s3://my-bucket/warehouse/db/events \
  --source-prefix   hdfs://namenode:8020/warehouse \
  --dest-prefix     s3://my-bucket/warehouse
```

### JSON output for scripting

```bash
iceberg-migrate migrate ... --json 2>/dev/null | jq .status
```

### All options

```
--table-location   S3 URI of the Iceberg table root        [required]
--source-prefix    Original path prefix to replace         [required]
--dest-prefix      New S3 path prefix                      [required]
--dry-run          Show planned changes, no writes
--verbose          Print per-file substitution counts
--glue-database    Override Glue database name
--glue-table       Override Glue table name
--json             Emit JSON to stdout, summary to stderr
--aws-region       AWS region for Glue (default: env)
```

---

## What Happens Under the Hood

```
S3 (original metadata)
  │
  ▼ Discovery
    Finds latest metadata.json, decompresses if .gz,
    loads manifest lists + manifests, captures Avro codec per file
  │
  ▼ Full graph loading
    Loads ALL snapshots — not just the current one.
    Historical snapshots reference old paths too.
  │
  ▼ Format-version gate
    Rejects format-version >= 4 before any writes.
  │
  ▼ Path rewriting
    metadata.json   → location, snapshot manifest-list paths
    manifest lists  → manifest_path records
    manifests       → file_path, referenced_data_file, deletion vectors
  │
  ▼ Validation
    Structural check + residual prefix scan + manifest count match
  │
  ▼ Write (bottom-up)
    manifests first → manifest lists → metadata.json last
    Writes to _migrated/ — originals untouched
    Strips .gz from output keys (Athena reads extension, not magic bytes)
  │
  ▼ Glue registration
    Creates or updates Glue table with table_type=ICEBERG,
    metadata_location pointing to _migrated/
```

**Non-destructive by design.** Migrated metadata lives in `_migrated/` alongside the original. Sync tools can overwrite the original metadata on the next run without affecting the migrated copy.

---

## Project Layout

```
src/iceberg_migrate/
├── cli.py                   # Typer CLI entry point
├── discovery/               # Locate, decompress, parse metadata from S3
├── rewrite/                 # Rewrite paths in JSON + Avro
├── validation/              # Pre-write structural + residual-prefix checks
├── writer/                  # Bottom-up S3 writer
├── catalog/                 # Pluggable catalog registration (Glue, REST)
└── output/                  # Rich human + JSON output

infra/
├── docker-compose.yml       # Local Lakekeeper, MinIO, HMS, seed containers
├── spark_jobs/              # PySpark verification scripts (Glue ETL + EMR)
└── terraform/               # AWS infra (S3, Glue DB, Athena, EMR app, IAM)
```

---

## Development

```bash
just test           # Unit tests (fast, no Docker)
just check          # Lint + types + unit tests
just seed-all       # Seed MinIO with test tables (requires Docker)
just test-local     # Integration tests against local Docker stack
```

### AWS integration tests

```bash
cp .env.example .env
# fill in AWS_TEST_BUCKET, AWS_REGION, etc.
just tf-apply       # provision S3, Glue DB, Athena workgroup, EMR app
just test-integration   # all 13 tests (Athena + Glue ETL + EMR Serverless)

# run individual engines
just test-athena
just test-glue
just test-emr
```

---

## Key Design Decisions

| Decision | Why |
|----------|-----|
| `fastavro` over `apache-avro` | 8× faster; no `IgnoredLogicalType` bugs with Iceberg manifest schemas |
| Direct S3 writes | PyIceberg's `StaticTable` is read-only — cannot write rewritten metadata back |
| Direct Glue API | Avoids s3fs/FileIO dependency; sets identical Iceberg parameters |
| `_migrated/` output dir | Original metadata may be overwritten by the next sync run; migrated copy is independent |
| Bottom-up write order | If a write fails mid-way, lower-level files are consistent; metadata pointer updated last |
| All-snapshot rewriting | Time-travel safety: historical snapshots reference old paths too |
| Codec round-trip preservation | Prevents silently changing deflate → null or vice versa across query engines |
| `.gz` extension stripping | Athena infers encoding from filename extension, not file magic bytes |
| Format-version gate | Rejects v4+ to prevent silent corruption from unknown metadata fields |

---

## License

MIT
