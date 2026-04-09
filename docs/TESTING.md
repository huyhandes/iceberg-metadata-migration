# Testing

## Quick Reference (justfile)

```bash
just test              # Unit tests (fast, no Docker)
just test-rest         # REST catalog local tests
just test-sql          # SQL catalog local tests
just test-hms          # HMS catalog local tests
just test-local        # All local catalog tests
just test-integration  # Full AWS round-trip (all engines: Athena + Glue + EMR)
just test-athena       # Athena integration tests only
just test-glue         # Glue ETL integration tests only
just test-emr          # EMR Serverless integration tests only
just test-all          # Everything
```

## Test Tiers

### Tier 1: Unit Tests (moto-mocked)

Fast tests using moto for S3/Glue mocking. No Docker or AWS credentials needed.

```bash
just test
# or: uv run pytest tests/ -x --ignore=tests/integration
```

| Module | Test File |
|--------|-----------|
| `discovery/locator.py` | `tests/test_discovery/test_locator.py` |
| `discovery/reader.py` | `tests/test_discovery/test_reader.py` |
| `rewrite/config.py` | `tests/test_rewrite/test_config.py` |
| `rewrite/metadata_rewriter.py` | `tests/test_rewrite/test_metadata_rewriter.py` |
| `rewrite/avro_rewriter.py` | `tests/test_rewrite/test_avro_rewriter.py` |
| `rewrite/graph_loader.py` | `tests/test_rewrite/test_graph_loader.py` |
| `rewrite/engine.py` | `tests/test_rewrite/test_engine.py` |
| `validation/validator.py` | `tests/test_validation/test_validator.py` |
| `writer/s3_writer.py` | `tests/test_writer/test_s3_writer.py` |
| `catalog/glue_registrar.py` | `tests/test_catalog/test_glue_registrar.py` |

Integration tests (moto): `tests/test_rewrite/test_integration.py`, `tests/test_integration/test_end_to_end.py`

### Tier 2: Local Catalog Tests (Docker)

Tests migration against real Iceberg metadata produced by different catalog types on MinIO. These tests exercise real catalog-produced metadata including gzip-compressed metadata.json (Lakekeeper) and deflate-coded Avro manifests (all Java catalogs).

**Prerequisites:**
- Docker running
- Relevant catalog seeded (see Infrastructure section)

```bash
just seed-rest && just test-rest     # REST (Lakekeeper)
just seed-sql && just test-sql       # SQL (SQLite)
just seed-hms && just test-hms       # HMS (Hive Metastore)
just seed-all && just test-local     # All catalogs
```

**Pytest markers:**

| Marker | Catalog | Docker Profile |
|--------|---------|---------------|
| `@pytest.mark.rest` | Lakekeeper REST | `rest` |
| `@pytest.mark.sql` | SQLite SQL | (none — MinIO only) |
| `@pytest.mark.hms` | Hive Metastore | `hms` |

### Tier 3: AWS Integration Tests (Multi-Engine)

Full round-trip: seed on MinIO -> sync to AWS S3 -> migrate -> engine-specific SELECT verification.

**Prerequisites:**
- Docker running + all catalogs seeded (`just seed-all`)
- `AWS_PROFILE` configured in `.env`
- Terraform infra applied (`just tf-apply`)
- `.env` vars: `AWS_TEST_BUCKET`, `GLUE_DATABASE`, `ATHENA_WORKGROUP`, `EMR_APPLICATION_ID`, `EMR_JOB_ROLE_ARN`, `GLUE_JOB_NAME`

Migration setup (sync + rewrite + Glue registration + Lake Formation per-table grants) is handled once by the session-scoped `migrated_tables` fixture in `conftest.py`. All three engine test files share this fixture.

#### Athena (pyiceberg GlueCatalog)

13 tests: 3 row count + 3 dimension join + 1 cross-catalog join + 3 time-travel snapshot 1 + 3 time-travel snapshot 2, parametrized across `rest_ns`, `sql_ns`, `hms_ns` namespaces. Queries execute via Athena SQL against the Glue Catalog. Time-travel uses `FOR TIMESTAMP AS OF TIMESTAMP '{iso_string}'`.

```bash
just test-athena
# or: uv run pytest tests/integration/test_athena_verify.py -m integration -v
```

#### Glue ETL 5.1

3 tests (1 test function parametrized across 3 namespaces). Submits `verify_glue.py` as a Glue ETL job run. Uses explicit Iceberg `glue_catalog` configuration — `--datalake-formats iceberg` only provides Iceberg JARs; the named catalog is wired via `--conf` entries (SparkCatalog + GlueCatalog impl + S3FileIO). Tables accessed as `glue_catalog.{db}.{table}`. Each test verifies: row count (10), dimension JOIN (5 rows), cross-catalog JOIN (3 rows), time-travel to snapshot 1 (5 rows, SUM 801.5), time-travel to snapshot 2 (5 rows, SUM 2387.25).

```bash
just test-glue
# or: uv run pytest tests/integration/test_glue_verify.py -m integration -v
```

#### EMR Serverless (EMR 7.12.0)

3 tests (1 test function parametrized across 3 namespaces). Submits `verify_emr.py` as an EMR Serverless Spark job. Uses identical `glue_catalog` Spark configuration to Glue ETL. Includes `--aws_region` argument so `boto3` uses the regional S3 endpoint (global endpoint times out in some regions). Same verification as Glue ETL: current state queries + time-travel to both snapshots.

```bash
just test-emr
# or: uv run pytest tests/integration/test_emr_verify.py -m integration -v
```

**Cleanup:** Tests clean up S3 objects and Glue tables in `finally` blocks.

**Total integration tests:** 19 (13 Athena + 3 Glue ETL + 3 EMR Serverless)

## Mocking Approach (Unit Tests)

### AWS Services (moto)

```python
from moto import mock_aws

@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        yield client
```

Shared fixtures in `tests/conftest.py`:
- `s3_client` — mocked S3 client
- `aws_clients` — mocked S3 + Glue with pre-created `testdb` database

### Avro Fixtures

Tests create Avro data in-memory using fastavro. See `tests/fixtures/v3_manifest_with_dv.py`.

## What Tests Verify

1. **Path rewriting correctness** — every path-bearing field is rewritten
2. **Avro round-trip** — serialize -> deserialize preserves schema and data
3. **Non-destructive writes** — output goes to `_migrated/` keys, originals untouched
4. **Validation gate** — rewritten files have zero residual source prefixes
5. **Count preservation** — manifest counts match before/after rewrite
6. **Idempotent registration** — Glue create vs. update behavior
7. **Error propagation** — S3/Glue failures produce correct exit codes
8. **Cross-catalog compatibility** — metadata from REST, SQL, HMS catalogs all migrate correctly
9. **Athena queryability** — migrated tables are queryable end-to-end via Athena SQL
10. **Compression handling** — gzip-compressed metadata.json is decompressed before parsing; compression suffix stripped from output keys
11. **Avro codec round-trip** — codec read from source Avro header is preserved in rewritten output
12. **Format-version gate** — format-version 4+ rejected with clear error before any rewriting
13. **Glue ETL queryability** — migrated tables are queryable via Glue ETL 5.1 Iceberg catalog
14. **EMR Serverless queryability** — migrated tables are queryable via EMR Serverless Spark (EMR 7.12.0)
15. **Cross-engine consistency** — all 3 engines read the same migrated metadata successfully
16. **Multi-snapshot time-travel** — historical snapshots are queryable after migration (Athena, Glue ETL, EMR Serverless all verify snapshot 1 and snapshot 2)
