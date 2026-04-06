# Multi-Engine Validation Design

## Goal

Verify that Iceberg tables migrated by the migration tool are queryable by all three data lake compute engines used in production: Athena, Glue ETL (Serverless), and EMR Serverless. Each engine independently validates SELECT, JOIN, and cross-catalog queries against migrated tables.

## Context

The migration tool rewrites Iceberg metadata paths and registers tables in AWS Glue. Tables are read-only after migration — they are populated by a daily/scheduled sync from a source system to S3. The integration test suite must confirm that all engines a data lake team would realistically use can read the migrated data without failure.

Current state: one test file (`test_athena_verify.py`) verifies Athena only with COUNT and spot-check queries. This design extends coverage to Glue ETL and EMR Serverless, adds JOIN queries, and promotes migration to a session-scoped fixture.

---

## Data Model

### `sample_table` (existing)
10 rows. Schema: `id (int64), name (string), city (string), amount (float64), created_at (timestamp)`.

### `cities` (new lookup table)
3 rows. Schema: `city_name (string), region (string), country (string)`.

| city_name | region           | country |
|-----------|------------------|---------|
| Hanoi     | Northern Vietnam | Vietnam |
| HCMC      | Southern Vietnam | Vietnam |
| Danang    | Central Vietnam  | Vietnam |

Seeded to MinIO alongside `sample_table`, migrated to S3 via the same CLI, registered in Glue.

**Total after migration:** 3 namespaces × 2 tables = 6 Glue tables.

---

## Query Scenarios

All three engines verify the same three query patterns per namespace:

| # | Name | Query | Expected Result |
|---|------|-------|-----------------|
| 1 | Row count | `SELECT COUNT(*) FROM {db}.{ns}_sample_table` | 10 |
| 2 | Dimension join | `SELECT s.name, c.region FROM {db}.{ns}_sample_table s JOIN {db}.{ns}_cities c ON s.city = c.city_name ORDER BY s.id LIMIT 5` | 5 rows, `region` populated from cities table |
| 3 | Cross-catalog join | `SELECT r.id, r.name, s.name FROM {db}.rest_ns_sample_table r JOIN {db}.sql_ns_sample_table s ON r.id = s.id WHERE r.id <= 3` | 3 rows, `r.name == s.name` for each row |

Query 3 proves that Iceberg tables migrated from different source catalogs (Lakekeeper REST vs SQL catalog) are readable together in a single query.

---

## Infrastructure (Terraform)

All new resources added to `infra/terraform/main.tf`. New variables added to `variables.tf` and `.env.example`.

### IAM Role — Glue ETL

```hcl
resource "aws_iam_role" "glue_job_role" { ... }
```

- Trust policy: `glue.amazonaws.com`
- Permissions: S3 GetObject/PutObject/ListBucket on test bucket, Glue GetTable/GetDatabase, CloudWatch Logs CreateLogGroup/CreateLogStream/PutLogEvents

### IAM Role — EMR Serverless

```hcl
resource "aws_iam_role" "emr_serverless_role" { ... }
```

- Trust policy: `emr-serverless.amazonaws.com`
- Same S3 and Glue permissions as Glue role

### EMR Serverless Application

```hcl
resource "aws_emrserverless_application" "iceberg_test" {
  name          = "iceberg-migration-test"
  release_label = "emr-7.0.0"
  type          = "SPARK"
  ...
}
```

Pre-provisioned and kept in STARTED state to avoid cold-start per test run. Output: `emr_application_id`.

### Glue ETL Job

```hcl
resource "aws_glue_job" "verify" {
  name     = "iceberg-migration-verify"
  role_arn = aws_iam_role.glue_job_role.arn
  command {
    script_location = "s3://${var.s3_bucket}/spark-jobs/verify_glue.py"
  }
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  ...
}
```

Output: `glue_job_name`.

### S3 Prefixes (no new bucket)

- `spark-jobs/` — Spark scripts uploaded at test session start
- `integration-results/` — JSON output written by jobs, read back by test assertions

### New `.env` Variables

```
GLUE_JOB_NAME=iceberg-migration-verify
EMR_APPLICATION_ID=<from terraform output>
EMR_JOB_ROLE_ARN=<arn of emr_serverless_role>
```

---

## Test Architecture

### Session Fixture (replaces per-test migration)

A session-scoped fixture `migrated_tables` in `conftest.py` runs once per `pytest` invocation:

1. Upload Spark scripts from `infra/spark_jobs/` to `s3://{bucket}/spark-jobs/`
2. Sync all 3 namespaces (rest, sql, hms) from MinIO to AWS S3 — both `sample_table` and `cities`
3. Run migration CLI for all 6 tables
4. `yield`
5. Cleanup: delete all S3 prefixes + drop all 6 Glue tables

Migration never runs more than once regardless of how many engine test files are collected.

### New Helpers in `conftest.py`

| Helper | Purpose |
|--------|---------|
| `upload_spark_scripts(s3_client)` | Upload `infra/spark_jobs/*.py` to S3 at session start |
| `run_glue_job(client, job_name, args, timeout=300)` | Start Glue job run → poll until SUCCEEDED/FAILED → return S3 output path |
| `run_emr_job(client, app_id, role_arn, script_s3, args, timeout=600)` | Start EMR Serverless job run → poll → return S3 output path |
| `read_job_results(s3_client, output_path)` | Read JSON lines written by Spark job, return `list[dict]` |

### Test Files

| File | Engine | Status |
|------|--------|--------|
| `tests/integration/test_athena_verify.py` | Athena engine v3 | Refactor — add JOIN queries, use `migrated_tables` fixture |
| `tests/integration/test_glue_verify.py` | Glue ETL 4.0 (Spark) | New |
| `tests/integration/test_emr_verify.py` | EMR Serverless 7.0 (Spark) | New |

All three files are parametrized over `CATALOG_CONFIGS` (rest, sql, hms) and depend on `migrated_tables`.

### Spark Scripts

**`infra/spark_jobs/verify_glue.py`**
- Uses `GlueContext` + `DynamicFrame`
- Reads Iceberg tables via Glue Data Catalog
- Runs all 3 query scenarios
- Writes results as JSON lines to `--output_path`
- Job arguments: `--output_path`, `--glue_database`, `--namespace`, `--cross_ns1`, `--cross_ns2`

**`infra/spark_jobs/verify_emr.py`**
- Plain `SparkSession` with `spark.sql.catalog.glue_catalog` configured
- Same 3 queries, same JSON output format
- Job arguments: same as above

Both scripts exit with non-zero code on any assertion failure so the test polling detects failure via job state (not just S3 content).

---

## File Changes Summary

| Path | Change |
|------|--------|
| `infra/terraform/main.tf` | Add IAM roles, EMR Serverless app, Glue job |
| `infra/terraform/variables.tf` | Add `emr_release_label` variable |
| `infra/seed/common.py` | Add `cities` table schema + seed data |
| `infra/seed/seed_rest.py` | Seed `cities` table in rest namespace |
| `infra/seed/seed_sql.py` | Seed `cities` table in sql namespace |
| `infra/seed/seed_hms.py` | Seed `cities` table in hms namespace |
| `infra/spark_jobs/verify_glue.py` | New — Glue ETL verification script |
| `infra/spark_jobs/verify_emr.py` | New — EMR Serverless verification script |
| `tests/integration/conftest.py` | Add session fixture, job helpers, new env vars |
| `tests/integration/test_athena_verify.py` | Refactor to session fixture + add JOIN queries |
| `tests/integration/test_glue_verify.py` | New |
| `tests/integration/test_emr_verify.py` | New |
| `.env.example` | Add `GLUE_JOB_NAME`, `EMR_APPLICATION_ID`, `EMR_JOB_ROLE_ARN` |

---

## Execution Flow

```
pytest -m integration
  │
  ├── [session start] migrated_tables fixture
  │     ├── upload spark scripts to S3
  │     ├── sync rest_ns (sample_table + cities) → S3 → migrate → Glue
  │     ├── sync sql_ns  (sample_table + cities) → S3 → migrate → Glue
  │     └── sync hms_ns  (sample_table + cities) → S3 → migrate → Glue
  │
  ├── test_athena_verify.py [rest, sql, hms]
  │     └── run_athena_query × 3 scenarios each
  │
  ├── test_glue_verify.py [rest, sql, hms]
  │     └── run_glue_job → poll → read S3 results → assert × 3 scenarios
  │
  └── test_emr_verify.py [rest, sql, hms]
        └── run_emr_job → poll → read S3 results → assert × 3 scenarios
  │
  └── [session end] cleanup: S3 + Glue
```

---

## Constraints

- Migrated tables are **read-only** — no INSERT/UPDATE/DELETE queries are run
- EMR Serverless application is pre-provisioned (not created per test run) to avoid 3–5 min cold-start
- All job results written to `s3://{bucket}/integration-results/{run_id}/` and cleaned up at session end
- Glue ETL uses 2 × G.1X workers (minimum viable, sufficient for 10-row test tables)
- EMR Serverless uses default auto-scaling (no pre-initialized capacity needed for test scale)
