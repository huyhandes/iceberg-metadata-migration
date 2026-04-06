# Infrastructure

## Docker Compose

Located at `infra/docker-compose.yml`. Uses profiles to selectively start services.

### Services

| Service | Profile | Port | Purpose |
|---------|---------|------|---------|
| `minio` | (always-on) | 9000, 9001 | S3-compatible object store |
| `minio-init` | (always-on) | — | Creates `warehouse` bucket |
| `lakekeeper` | `rest` | 8181 | Iceberg REST catalog |
| `hive-metastore` | `hms` | 9083 | Hive Metastore catalog |
| `hms-postgres` | `hms` | — | Backing database for HMS |

### Usage

```bash
# Start everything
just infra-up

# Start specific profile
docker compose -f infra/docker-compose.yml --profile rest up -d

# Stop and clean
just infra-down
```

### MinIO credentials

- Endpoint: `http://localhost:9000`
- Console: `http://localhost:9001`
- Access key: `minioadmin`
- Secret key: `minioadmin`
- Bucket: `warehouse`

## Terraform (AWS)

Located at `infra/terraform/main.tf`. Provisions long-lived AWS resources for integration testing.

### Resources

| Resource | Name/Type | Purpose |
|----------|-----------|---------|
| S3 Bucket | (from `var.s3_bucket`) | Test data, Athena results, Spark scripts |
| Glue Database | `iceberg_migration_test` | Target catalog for migrated tables |
| Athena Workgroup | `iceberg-migration-test` | Query engine (v3) for verification |
| Lake Formation | `IAM_ALLOWED_PRINCIPALS` on DB | Database-level access for all IAM principals |
| Glue ETL Job | `iceberg-migration-verify` | Glue 5.1, G.1X x 2 workers, 15 min timeout, max_concurrent_runs=3 |
| Glue IAM Role | `iceberg-migration-glue-job-role` | S3 + Glue + LakeFormation + CloudWatch |
| EMR Serverless App | `iceberg-migration-test` | EMR 7.12.0 Spark application (pre-provisioned) |
| EMR IAM Role | `iceberg-migration-emr-serverless-role` | S3 + Glue + LakeFormation + CloudWatch |

### Usage

```bash
just tf-init     # Initialize backend
just tf-plan     # Preview changes
just tf-apply    # Apply changes
just tf-destroy  # Tear down (careful!)
```

All `just tf-*` commands inject the `AWS_PROFILE` from `.env` automatically.

### Configuration

All environment-specific values are configured via `.env` (see `.env.example`):
- `AWS_PROFILE` — AWS CLI profile
- `AWS_REGION` — AWS region
- `AWS_TEST_BUCKET` — S3 bucket for test data and Athena results
- `EMR_APPLICATION_ID` — EMR Serverless application ID (output from `tf-apply`)
- `EMR_JOB_ROLE_ARN` — IAM role ARN for EMR Serverless jobs (output from `tf-apply`)
- `GLUE_JOB_NAME` — Glue ETL job name (`iceberg-migration-verify` by default)
- Terraform backend is configured via `infra/terraform/backend.tfvars` (see `backend.tfvars.example`)

### Glue ETL Configuration

The Glue ETL job uses Glue 5.1 with `--datalake-formats iceberg`, which provides Iceberg JARs on the classpath. However, this flag alone only configures `spark_catalog` as a Hive metastore bridge — it does NOT create a named Iceberg catalog backed by Glue. To query Iceberg tables registered in Glue Catalog, the job requires explicit `--conf` entries:

```
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

Tables are accessed as `glue_catalog.{db}.{table}` — identical naming to EMR Serverless. `max_concurrent_runs=3` allows all three namespace tests to execute in parallel.

### EMR Serverless Configuration

The EMR Serverless application uses release label `emr-7.12.0`, which includes built-in Iceberg support. Spark submit parameters configure `glue_catalog` identically to the Glue ETL job (same four `--conf` entries). The `verify_emr.py` script accepts `--aws_region` as a CLI argument, which is passed from the test fixture to ensure `boto3` uses the regional S3 endpoint — the global endpoint times out in some regions (e.g., ap-southeast-5 Jakarta).

## Seed Scripts

Located at `infra/seed/`. Create real Iceberg tables through different catalogs into MinIO.

| Script | Catalog | Namespace | Docker Profile |
|--------|---------|-----------|---------------|
| `seed_rest.py` | Lakekeeper REST | `rest_ns` | `rest` |
| `seed_sql.py` | SQLite SQL | `sql_ns` | (none) |
| `seed_hms.py` | Hive Metastore | `hms_ns` | `hms` |

Each writes to `s3://warehouse/{namespace}/sample_table/` on MinIO. Namespaces are isolated so all catalogs coexist without conflicts.

## Spark Verification Scripts

Located at `infra/spark_jobs/`. Run SQL verification queries against migrated Iceberg tables in two Spark environments.

| Script | Engine | Location | Purpose |
|--------|--------|----------|---------|
| `verify_glue.py` | Glue ETL 5.1 | `infra/spark_jobs/` | 3 queries via `glue_catalog.{db}.{table}` — row count, dimension JOIN, cross-catalog JOIN |
| `verify_emr.py` | EMR Serverless 7.12.0 | `infra/spark_jobs/` | Same 3 queries; includes `--aws_region` argument for regional S3 endpoint |

Scripts are uploaded to `s3://{bucket}/spark-jobs/` by the `migrated_tables` fixture in `conftest.py` before test runs.

### AWS Documentation References

- [Querying Iceberg tables in Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
- [Glue Catalog as Iceberg catalog](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-glue-catalog.html)
- [Glue ETL with Iceberg](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)
- [EMR Serverless Spark](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html)
- [Glue CreateTable API](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html)
- [Athena engine versions](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference.html)

A successful query from any of the three engines (Athena, Glue ETL, EMR Serverless) validates the entire migration chain: Glue registration -> metadata.json -> Avro manifests -> data file accessibility.
