# CLI Reference

## Command: `iceberg-migrate migrate`

Rewrite Iceberg metadata paths and register the table in AWS Glue Catalog.

### Required Flags

| Flag | Description | Example |
|------|-------------|---------|
| `--table-location` | S3 URI of the Iceberg table root | `s3://bucket/warehouse/db/table` |
| `--source-prefix` | Original path prefix to replace | `s3a://minio-bucket/warehouse` |
| `--dest-prefix` | New S3 path prefix | `s3://aws-bucket/warehouse` |

### Optional Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--dry-run` | `false` | Show planned changes without writing |
| `--verbose` | `false` | Print per-file path substitution counts |
| `--glue-database` | Derived from table location | Override Glue database name |
| `--glue-table` | Derived from table location | Override Glue table name |
| `--json` | `false` | Emit JSON to stdout; human summary to stderr |
| `--aws-region` | `AWS_DEFAULT_REGION` or `us-east-1` | AWS region for Glue |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Partial failure (some writes completed) |
| 2 | Fatal error (failed before writes) |

### Examples

**Basic migration:**
```bash
iceberg-migrate migrate \
  --table-location s3://my-bucket/warehouse/analytics/events \
  --source-prefix s3a://minio-prod/warehouse \
  --dest-prefix s3://my-bucket/warehouse
```

**HDFS source (non-S3 source prefix):**
```bash
iceberg-migrate migrate \
  --table-location s3://my-bucket/warehouse/analytics/events \
  --source-prefix hdfs://namenode:8020/warehouse \
  --dest-prefix s3://my-bucket/warehouse
```

**Dry run with verbose output:**
```bash
iceberg-migrate migrate \
  --table-location s3://my-bucket/warehouse/analytics/events \
  --source-prefix s3a://old-bucket/warehouse \
  --dest-prefix s3://my-bucket/warehouse \
  --dry-run --verbose
```

**JSON output for scripting:**
```bash
iceberg-migrate migrate \
  --table-location s3://my-bucket/warehouse/analytics/events \
  --source-prefix s3a://old-bucket/warehouse \
  --dest-prefix s3://my-bucket/warehouse \
  --json 2>/dev/null | jq .status
```

**Custom Glue names:**
```bash
iceberg-migrate migrate \
  --table-location s3://my-bucket/warehouse/analytics/events \
  --source-prefix s3a://old-bucket/warehouse \
  --dest-prefix s3://my-bucket/warehouse \
  --glue-database my_analytics \
  --glue-table user_events
```
