# Multi-Engine Iceberg Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the integration test suite so Athena, Glue ETL, and EMR Serverless each independently verify that migrated Iceberg tables are queryable with SELECT, JOIN, and cross-catalog operations.

**Architecture:** A session-scoped pytest fixture migrates all 3 namespaces × 2 tables once per test run. Three test files (one per engine) consume that fixture independently. Glue ETL and EMR Serverless jobs run PySpark scripts uploaded to S3 at session start, write results as JSON, and the Python test reads and asserts the results.

**Tech Stack:** Python 3.12, pytest, boto3, Terraform (AWS provider ~5.0), PySpark (via Glue 4.0 / EMR 7.x), PyIceberg, AWS Glue ETL, AWS EMR Serverless.

**Spec:** `docs/specs/multi-engine-validation-design.md`

---

## File Map

| Path | Change | Responsibility |
|------|--------|---------------|
| `infra/seed/common.py` | Modify | Add `CITIES_TABLE_NAME`, `CITIES_SCHEMA`, `cities_data()` |
| `infra/seed/seed_rest.py` | Modify | Seed `cities` table in REST/Lakekeeper catalog |
| `infra/seed/seed_sql.py` | Modify | Seed `cities` table in SQL catalog |
| `infra/seed/seed_hms.py` | Modify | Seed `cities` table in HMS namespace |
| `infra/terraform/main.tf` | Modify | Add IAM roles, EMR Serverless app, Glue job |
| `infra/terraform/variables.tf` | Modify | Add `emr_release_label` variable |
| `infra/spark_jobs/verify_glue.py` | Create | PySpark Glue ETL verification script |
| `infra/spark_jobs/verify_emr.py` | Create | PySpark EMR Serverless verification script |
| `tests/integration/conftest.py` | Modify | Session fixture, Glue/EMR job helpers, new env vars |
| `tests/integration/test_athena_verify.py` | Modify | Use session fixture, add JOIN queries |
| `tests/integration/test_glue_verify.py` | Create | Glue ETL integration test |
| `tests/integration/test_emr_verify.py` | Create | EMR Serverless integration test |
| `.env.example` | Modify | Add `GLUE_JOB_NAME`, `EMR_APPLICATION_ID`, `EMR_JOB_ROLE_ARN` |

---

## Task 1: Add `cities` table to seed data

**Files:**
- Modify: `infra/seed/common.py`
- Modify: `infra/seed/seed_rest.py`
- Modify: `infra/seed/seed_sql.py`
- Modify: `infra/seed/seed_hms.py`

- [ ] **Step 1: Add `cities` schema and data to `common.py`**

Add to `infra/seed/common.py` after the existing `TABLE_NAME` constant:

```python
CITIES_TABLE_NAME = "cities"

CITIES_SCHEMA = pa.schema(
    [
        pa.field("city_name", pa.string()),
        pa.field("region", pa.string()),
        pa.field("country", pa.string()),
    ]
)


def cities_data() -> pa.Table:
    """Return a PyArrow table with 3 city rows for the dimension join test."""
    return pa.table(
        {
            "city_name": ["Hanoi", "HCMC", "Danang"],
            "region": ["Northern Vietnam", "Southern Vietnam", "Central Vietnam"],
            "country": ["Vietnam", "Vietnam", "Vietnam"],
        },
        schema=CITIES_SCHEMA,
    )
```

- [ ] **Step 2: Update `seed_rest.py` to seed `cities`**

In `infra/seed/seed_rest.py`, add `CITIES_TABLE_NAME`, `CITIES_SCHEMA`, `cities_data` to the import from `infra.seed.common`:

```python
from infra.seed.common import (
    CATALOG_NAMESPACES,
    CITIES_SCHEMA,
    CITIES_TABLE_NAME,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
    TABLE_NAME,
    TABLE_SCHEMA,
    WAREHOUSE,
    cities_data,
    s3_properties,
    sample_data,
)
```

Then at the end of `main()`, after the `sample_table` verify block, add:

```python
    # Seed cities dimension table
    cities_id = (namespace, CITIES_TABLE_NAME)
    try:
        catalog.drop_table(cities_id)
        print(f"Dropped existing table: {namespace}.{CITIES_TABLE_NAME}")
    except Exception:
        pass

    cities_prefix = f"{namespace}/{CITIES_TABLE_NAME}/"
    for page in paginator.paginate(Bucket="warehouse", Prefix=cities_prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket="warehouse", Delete={"Objects": objects})

    cities_table = catalog.create_table(
        identifier=cities_id,
        schema=CITIES_SCHEMA,
        location=f"{WAREHOUSE}/{namespace}/{CITIES_TABLE_NAME}",
    )
    cities_table.io = PyArrowFileIO(
        {
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.region": MINIO_REGION,
        }
    )
    cities_table.append(cities_data())
    print(f"Created {namespace}.{CITIES_TABLE_NAME}")
```

- [ ] **Step 3: Update `seed_sql.py` to seed `cities`**

Add to the import from `infra.seed.common`:

```python
from infra.seed.common import (
    CATALOG_NAMESPACES,
    CITIES_SCHEMA,
    CITIES_TABLE_NAME,
    TABLE_NAME,
    TABLE_SCHEMA,
    WAREHOUSE,
    cities_data,
    s3_properties,
    sample_data,
)
```

At the end of `main()`, after the `sample_table` verify block:

```python
    # Seed cities dimension table
    cities_id = (namespace, CITIES_TABLE_NAME)
    try:
        catalog.drop_table(cities_id)
    except Exception:
        pass

    cities_table = catalog.create_table(
        identifier=cities_id,
        schema=CITIES_SCHEMA,
        location=f"{WAREHOUSE}/{namespace}/{CITIES_TABLE_NAME}",
    )
    cities_table.append(cities_data())
    print(f"Created {namespace}.{CITIES_TABLE_NAME}")
```

- [ ] **Step 4: Update `seed_hms.py` to seed `cities`** (identical to sql pattern)

Add to the import from `infra.seed.common`:

```python
from infra.seed.common import (
    CATALOG_NAMESPACES,
    CITIES_SCHEMA,
    CITIES_TABLE_NAME,
    TABLE_NAME,
    TABLE_SCHEMA,
    WAREHOUSE,
    cities_data,
    s3_properties,
    sample_data,
)
```

At the end of `main()`, after the `sample_table` verify block:

```python
    # Seed cities dimension table
    cities_id = (namespace, CITIES_TABLE_NAME)
    try:
        catalog.drop_table(cities_id)
    except Exception:
        pass

    cities_table = catalog.create_table(
        identifier=cities_id,
        schema=CITIES_SCHEMA,
        location=f"{WAREHOUSE}/{namespace}/{CITIES_TABLE_NAME}",
    )
    cities_table.append(cities_data())
    print(f"Created {namespace}.{CITIES_TABLE_NAME}")
```

- [ ] **Step 5: Re-seed all catalogs and verify**

```bash
just infra-up
just seed-all
```

Expected: each seed script prints two `Created {namespace}.{table}` lines — one for `sample_table`, one for `cities`.

- [ ] **Step 6: Commit**

```bash
git add infra/seed/common.py infra/seed/seed_rest.py infra/seed/seed_sql.py infra/seed/seed_hms.py
git commit -m "feat: seed cities dimension table in all 3 catalog namespaces"
```

---

## Task 2: Add Terraform infrastructure

**Files:**
- Modify: `infra/terraform/main.tf`
- Modify: `infra/terraform/variables.tf`

- [ ] **Step 1: Add `emr_release_label` variable to `variables.tf`**

```hcl
variable "emr_release_label" {
  description = "EMR release label for the Serverless Spark application"
  type        = string
  default     = "emr-7.0.0"
}
```

- [ ] **Step 2: Add Glue ETL IAM role to `main.tf`**

Append to `infra/terraform/main.tf`:

```hcl
# ---------------------------------------------------------------------------
# Glue ETL job IAM role
# ---------------------------------------------------------------------------

resource "aws_iam_role" "glue_job_role" {
  name = "iceberg-migration-glue-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Project = "iceberg-migration-tool" }
}

resource "aws_iam_role_policy" "glue_job_policy" {
  name = "iceberg-migration-glue-job-policy"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}",
          "arn:aws:s3:::${var.s3_bucket}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions", "glue:GetTables"]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:*:*:/aws-glue/*"]
      },
    ]
  })
}
```

- [ ] **Step 3: Add EMR Serverless IAM role and application to `main.tf`**

```hcl
# ---------------------------------------------------------------------------
# EMR Serverless IAM role
# ---------------------------------------------------------------------------

resource "aws_iam_role" "emr_serverless_role" {
  name = "iceberg-migration-emr-serverless-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Project = "iceberg-migration-tool" }
}

resource "aws_iam_role_policy" "emr_serverless_policy" {
  name = "iceberg-migration-emr-serverless-policy"
  role = aws_iam_role.emr_serverless_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}",
          "arn:aws:s3:::${var.s3_bucket}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions", "glue:GetTables"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"]
        Resource = ["*"]
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# EMR Serverless application (pre-provisioned, stays STARTED between test runs)
# ---------------------------------------------------------------------------

resource "aws_emrserverless_application" "iceberg_test" {
  name          = "iceberg-migration-test"
  release_label = var.emr_release_label
  type          = "SPARK"

  tags = { Project = "iceberg-migration-tool" }
}
```

- [ ] **Step 4: Add Glue ETL job definition to `main.tf`**

```hcl
# ---------------------------------------------------------------------------
# Glue ETL job definition
# ---------------------------------------------------------------------------

resource "aws_glue_job" "verify" {
  name     = "iceberg-migration-verify"
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket}/spark-jobs/verify_glue.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"             = "python"
    "--enable-glue-datacatalog"  = "true"
    "--datalake-formats"         = "iceberg"
    "--enable-job-insights"      = "false"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10

  tags = { Project = "iceberg-migration-tool" }
}
```

- [ ] **Step 5: Add new outputs to `main.tf`**

```hcl
output "emr_application_id" {
  value = aws_emrserverless_application.iceberg_test.id
}

output "emr_job_role_arn" {
  value = aws_iam_role.emr_serverless_role.arn
}

output "glue_job_name" {
  value = aws_glue_job.verify.name
}
```

- [ ] **Step 6: Plan and apply**

```bash
just tf-plan   # review: should show 6 new resources
just tf-apply
```

Expected new resources:
- `aws_iam_role.glue_job_role`
- `aws_iam_role_policy.glue_job_policy`
- `aws_iam_role.emr_serverless_role`
- `aws_iam_role_policy.emr_serverless_policy`
- `aws_emrserverless_application.iceberg_test`
- `aws_glue_job.verify`

After apply, capture outputs and add to `.env`:

```bash
terraform -chdir=infra/terraform output emr_application_id   # copy to EMR_APPLICATION_ID
terraform -chdir=infra/terraform output emr_job_role_arn     # copy to EMR_JOB_ROLE_ARN
terraform -chdir=infra/terraform output glue_job_name        # should be: iceberg-migration-verify
```

- [ ] **Step 7: Commit**

```bash
git add infra/terraform/main.tf infra/terraform/variables.tf
git commit -m "feat(infra): add Glue ETL job, EMR Serverless app, and IAM roles"
```

---

## Task 3: Update `.env.example`

**Files:**
- Modify: `.env.example`

- [ ] **Step 1: Add new env vars**

Replace the contents of `.env.example` with:

```bash
# AWS Configuration
AWS_PROFILE=your-aws-profile
AWS_REGION=your-region
TF_STATE_BUCKET=your-terraform-state-bucket
AWS_TEST_BUCKET=your-test-bucket

# Glue / Athena (optional — defaults are fine for most setups)
# GLUE_DATABASE=iceberg_migration_test
# ATHENA_WORKGROUP=iceberg-migration-test

# Glue ETL + EMR Serverless (required for multi-engine integration tests)
GLUE_JOB_NAME=iceberg-migration-verify
EMR_APPLICATION_ID=<from: terraform output emr_application_id>
EMR_JOB_ROLE_ARN=<from: terraform output emr_job_role_arn>
```

- [ ] **Step 2: Commit**

```bash
git add .env.example
git commit -m "docs: add GLUE_JOB_NAME, EMR_APPLICATION_ID, EMR_JOB_ROLE_ARN to .env.example"
```

---

## Task 4: Write Glue ETL verification Spark script

**Files:**
- Create: `infra/spark_jobs/verify_glue.py`

- [ ] **Step 1: Create `infra/spark_jobs/` directory and write the script**

```bash
mkdir -p infra/spark_jobs
```

Create `infra/spark_jobs/verify_glue.py`:

```python
"""Glue ETL verification script for migrated Iceberg tables.

Runs 3 query scenarios against migrated Glue Catalog tables:
  1. Row count on {namespace}_sample_table  → expect 10
  2. Dimension JOIN: {namespace}_sample_table ⋈ {namespace}_cities on city_name → expect 5 rows
  3. Cross-catalog JOIN: rest_ns_sample_table ⋈ sql_ns_sample_table on id → expect 3 rows

Writes a JSON object to s3://{output_path}/results.json for test assertions.

Required job arguments (passed via --key value in Glue job run):
  --output_path    S3 URI prefix for output (e.g., s3://bucket/integration-results/glue/run-id/)
  --glue_database  Glue database name (e.g., iceberg_migration_test)
  --namespace      Primary namespace (e.g., rest_ns)
  --cross_ns1      First namespace for cross-catalog join (e.g., rest_ns)
  --cross_ns2      Second namespace for cross-catalog join (e.g., sql_ns)

Prerequisites:
  Glue job must have --datalake-formats iceberg in default_arguments (set via Terraform).
  This enables Iceberg extensions on the SparkSession automatically.
"""

from __future__ import annotations

import json
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# ---------------------------------------------------------------------------
# Glue job bootstrap
# ---------------------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "output_path", "glue_database", "namespace", "cross_ns1", "cross_ns2"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

db = args["glue_database"]
ns = args["namespace"]
cross_ns1 = args["cross_ns1"]
cross_ns2 = args["cross_ns2"]
output_path = args["output_path"].rstrip("/")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _s3_parts(uri: str) -> tuple[str, str]:
    """Return (bucket, key) from an s3:// URI."""
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def put_results(s3_uri_prefix: str, results: dict) -> None:
    """Write results dict as JSON to {s3_uri_prefix}/results.json."""
    bucket, key_prefix = _s3_parts(s3_uri_prefix)
    key = key_prefix.rstrip("/") + "/results.json"
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(results).encode(),
    )


# ---------------------------------------------------------------------------
# Query 1: Row count
# ---------------------------------------------------------------------------

count_rows = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM `{db}`.`{ns}_sample_table`"
).collect()
row_count = int(count_rows[0]["cnt"])

# ---------------------------------------------------------------------------
# Query 2: Dimension JOIN — sample_table ⋈ cities
# ---------------------------------------------------------------------------

join_rows = spark.sql(
    f"""
    SELECT s.name, c.region
    FROM `{db}`.`{ns}_sample_table` s
    JOIN `{db}`.`{ns}_cities` c ON s.city = c.city_name
    ORDER BY s.id
    LIMIT 5
    """
).collect()
join_results = [{"name": r["name"], "region": r["region"]} for r in join_rows]

# ---------------------------------------------------------------------------
# Query 3: Cross-catalog JOIN — rest_ns ⋈ sql_ns
# ---------------------------------------------------------------------------

cross_rows = spark.sql(
    f"""
    SELECT r.id, r.name AS rest_name, s.name AS sql_name
    FROM `{db}`.`{cross_ns1}_sample_table` r
    JOIN `{db}`.`{cross_ns2}_sample_table` s ON r.id = s.id
    WHERE r.id <= 3
    ORDER BY r.id
    """
).collect()
cross_results = [
    {"id": r["id"], "rest_name": r["rest_name"], "sql_name": r["sql_name"]}
    for r in cross_rows
]

# ---------------------------------------------------------------------------
# Write results
# ---------------------------------------------------------------------------

results = {
    "row_count": row_count,
    "join_rows": join_results,
    "cross_join_rows": cross_results,
}
put_results(output_path, results)

job.commit()
```

- [ ] **Step 2: Commit**

```bash
git add infra/spark_jobs/verify_glue.py
git commit -m "feat: add Glue ETL verification Spark script"
```

---

## Task 5: Write EMR Serverless verification Spark script

**Files:**
- Create: `infra/spark_jobs/verify_emr.py`

- [ ] **Step 1: Create `infra/spark_jobs/verify_emr.py`**

```python
"""EMR Serverless verification script for migrated Iceberg tables.

Same 3 query scenarios as verify_glue.py but using plain SparkSession
with the Glue Data Catalog configured as the Iceberg catalog (glue_catalog).

Tables are referenced as: glue_catalog.{db}.{ns}_sample_table

Writes a JSON object to {output_path}/results.json for test assertions.

Arguments (passed via entryPointArguments in the EMR Serverless job run):
  --output_path    S3 URI prefix for output
  --glue_database  Glue database name
  --namespace      Primary namespace
  --cross_ns1      First namespace for cross-catalog join
  --cross_ns2      Second namespace for cross-catalog join

The EMR Serverless job run must include these sparkSubmitParameters:
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
  --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
These are passed by run_emr_job() in conftest.py.
"""

from __future__ import annotations

import argparse
import json

import boto3
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser(description="EMR Serverless Iceberg validation")
parser.add_argument("--output_path", required=True)
parser.add_argument("--glue_database", required=True)
parser.add_argument("--namespace", required=True)
parser.add_argument("--cross_ns1", required=True)
parser.add_argument("--cross_ns2", required=True)
args = parser.parse_args()

db = args.glue_database
ns = args.namespace
cross_ns1 = args.cross_ns1
cross_ns2 = args.cross_ns2
output_path = args.output_path.rstrip("/")

# ---------------------------------------------------------------------------
# SparkSession — Iceberg catalog configured via sparkSubmitParameters
# ---------------------------------------------------------------------------

spark = SparkSession.builder.appName("iceberg-emr-verify").getOrCreate()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _s3_parts(uri: str) -> tuple[str, str]:
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def put_results(s3_uri_prefix: str, results: dict) -> None:
    bucket, key_prefix = _s3_parts(s3_uri_prefix)
    key = key_prefix.rstrip("/") + "/results.json"
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(results).encode(),
    )


# ---------------------------------------------------------------------------
# Query 1: Row count
# ---------------------------------------------------------------------------

count_rows = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM glue_catalog.`{db}`.`{ns}_sample_table`"
).collect()
row_count = int(count_rows[0]["cnt"])

# ---------------------------------------------------------------------------
# Query 2: Dimension JOIN
# ---------------------------------------------------------------------------

join_rows = spark.sql(
    f"""
    SELECT s.name, c.region
    FROM glue_catalog.`{db}`.`{ns}_sample_table` s
    JOIN glue_catalog.`{db}`.`{ns}_cities` c ON s.city = c.city_name
    ORDER BY s.id
    LIMIT 5
    """
).collect()
join_results = [{"name": r["name"], "region": r["region"]} for r in join_rows]

# ---------------------------------------------------------------------------
# Query 3: Cross-catalog JOIN
# ---------------------------------------------------------------------------

cross_rows = spark.sql(
    f"""
    SELECT r.id, r.name AS rest_name, s.name AS sql_name
    FROM glue_catalog.`{db}`.`{cross_ns1}_sample_table` r
    JOIN glue_catalog.`{db}`.`{cross_ns2}_sample_table` s ON r.id = s.id
    WHERE r.id <= 3
    ORDER BY r.id
    """
).collect()
cross_results = [
    {"id": r["id"], "rest_name": r["rest_name"], "sql_name": r["sql_name"]}
    for r in cross_rows
]

# ---------------------------------------------------------------------------
# Write results
# ---------------------------------------------------------------------------

results = {
    "row_count": row_count,
    "join_rows": join_results,
    "cross_join_rows": cross_results,
}
put_results(output_path, results)

spark.stop()
```

- [ ] **Step 2: Commit**

```bash
git add infra/spark_jobs/verify_emr.py
git commit -m "feat: add EMR Serverless verification Spark script"
```

---

## Task 6: Refactor `conftest.py`

**Files:**
- Modify: `tests/integration/conftest.py`

Add new env var constants, two new AWS client fixtures (`emrserverless_client`, `glue_job_client`), four new helpers (`upload_spark_scripts`, `run_glue_job`, `run_emr_job`, `read_job_results`), and the session `migrated_tables` fixture. The existing helpers (`sync_minio_to_s3`, `run_athena_query`, `cleanup_*`) remain unchanged.

- [ ] **Step 1: Add new imports and constants**

At the top of `tests/integration/conftest.py`, add to the existing imports:

```python
import json
import pathlib
import uuid
from typing import Any
```

After the existing constants block, add:

```python
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "iceberg-migration-verify")
EMR_APPLICATION_ID = os.environ.get("EMR_APPLICATION_ID", "")
EMR_JOB_ROLE_ARN = os.environ.get("EMR_JOB_ROLE_ARN", "")

SPARK_JOBS_DIR = pathlib.Path(__file__).parent.parent.parent / "infra" / "spark_jobs"

NAMESPACES = ["rest_ns", "sql_ns", "hms_ns"]
TABLES_TO_MIGRATE = ["sample_table", "cities"]
```

- [ ] **Step 2: Add `emrserverless_client` and `glue_job_client` fixtures**

```python
@pytest.fixture(scope="session")
def emrserverless_client():
    """Boto3 EMR Serverless client for real AWS."""
    return boto3.client("emr-serverless", region_name=AWS_REGION)


@pytest.fixture(scope="session")
def glue_job_client():
    """Boto3 Glue client for job runs (reuses region from env)."""
    return boto3.client("glue", region_name=AWS_REGION)
```

- [ ] **Step 3: Add `upload_spark_scripts` helper**

```python
def upload_spark_scripts(s3_client: S3Client) -> None:
    """Upload all .py files from infra/spark_jobs/ to s3://{AWS_BUCKET}/spark-jobs/.

    Called once at session start so Glue and EMR Serverless jobs can access the scripts.
    """
    for script_path in SPARK_JOBS_DIR.glob("*.py"):
        key = f"spark-jobs/{script_path.name}"
        s3_client.put_object(
            Bucket=AWS_BUCKET,
            Key=key,
            Body=script_path.read_bytes(),
        )
        print(f"Uploaded {script_path.name} to s3://{AWS_BUCKET}/{key}")
```

- [ ] **Step 4: Add `run_glue_job` helper**

```python
def run_glue_job(
    client: Any,
    job_name: str,
    arguments: dict[str, str],
    timeout: int = 600,
) -> dict[str, Any]:
    """Start a Glue job run and poll until SUCCEEDED or FAILED.

    Args:
        client: boto3 Glue client.
        job_name: Name of the Glue job definition.
        arguments: Job arguments dict (e.g., {"--output_path": "s3://..."}).
        timeout: Max seconds to wait.

    Returns:
        The final JobRun dict from get_job_run.

    Raises:
        RuntimeError: If the job enters a terminal failure state.
        TimeoutError: If the job does not complete within timeout seconds.
    """
    run = client.start_job_run(JobName=job_name, Arguments=arguments)
    run_id = run["JobRunId"]

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get_job_run(JobName=job_name, RunId=run_id)
        state = response["JobRun"]["JobRunState"]

        if state == "SUCCEEDED":
            return response["JobRun"]
        if state in ("FAILED", "STOPPED", "TIMEOUT", "ERROR"):
            error_msg = response["JobRun"].get("ErrorMessage", "no error message")
            raise RuntimeError(f"Glue job run {run_id} {state}: {error_msg}")

        time.sleep(15)

    raise TimeoutError(f"Glue job run {run_id} timed out after {timeout}s")
```

- [ ] **Step 5: Add `run_emr_job` helper**

```python
EMR_SERVERLESS_SPARK_CONF = (
    "--conf spark.sql.extensions="
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    " --conf spark.sql.catalog.glue_catalog="
    "org.apache.iceberg.spark.SparkCatalog"
    " --conf spark.sql.catalog.glue_catalog.catalog-impl="
    "org.apache.iceberg.aws.glue.GlueCatalog"
    " --conf spark.sql.catalog.glue_catalog.io-impl="
    "org.apache.iceberg.aws.s3.S3FileIO"
)


def run_emr_job(
    client: Any,
    application_id: str,
    execution_role_arn: str,
    script_s3_uri: str,
    entry_point_args: list[str],
    timeout: int = 900,
) -> dict[str, Any]:
    """Start an EMR Serverless job run and poll until SUCCESS or terminal failure.

    Args:
        client: boto3 EMR Serverless client.
        application_id: Pre-provisioned EMR Serverless application ID.
        execution_role_arn: IAM role ARN the job assumes.
        script_s3_uri: S3 URI of the PySpark entry point script.
        entry_point_args: List of string arguments passed to the script.
        timeout: Max seconds to wait.

    Returns:
        The final jobRun dict from get_job_run.

    Raises:
        RuntimeError: If the job enters a terminal failure state.
        TimeoutError: If the job does not complete within timeout seconds.
    """
    run = client.start_job_run(
        applicationId=application_id,
        executionRoleArn=execution_role_arn,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": script_s3_uri,
                "entryPointArguments": entry_point_args,
                "sparkSubmitParameters": EMR_SERVERLESS_SPARK_CONF,
            }
        },
    )
    run_id = run["jobRunId"]

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get_job_run(applicationId=application_id, jobRunId=run_id)
        state = response["jobRun"]["state"]

        if state == "SUCCESS":
            return response["jobRun"]
        if state in ("FAILED", "CANCELLED", "CANCELLING"):
            raise RuntimeError(f"EMR Serverless job run {run_id} {state}")

        time.sleep(20)

    raise TimeoutError(f"EMR Serverless job run {run_id} timed out after {timeout}s")
```

- [ ] **Step 6: Add `read_job_results` helper**

```python
def read_job_results(s3_client: S3Client, output_s3_uri: str) -> dict[str, Any]:
    """Read the results.json written by a Spark verification job.

    Args:
        s3_client: boto3 S3 client.
        output_s3_uri: S3 URI prefix used as the job's output_path argument.

    Returns:
        Parsed JSON dict written by the Spark script.
    """
    without_scheme = output_s3_uri.removeprefix("s3://")
    bucket, _, prefix = without_scheme.partition("/")
    key = prefix.rstrip("/") + "/results.json"
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)
```

- [ ] **Step 7: Add `migrated_tables` session fixture**

This fixture replaces per-test sync/migrate logic. It runs once per `pytest -m integration` invocation.

```python
@pytest.fixture(scope="session")
def migrated_tables(
    minio_client: S3Client,
    aws_s3_client: S3Client,
    glue_client: GlueClient,
) -> list[tuple[str, str, str]]:
    """Session fixture: upload spark scripts, sync and migrate all tables, yield, cleanup.

    Yields:
        List of (namespace, table_name, glue_table_name) for every migrated table.
    """
    from typer.testing import CliRunner
    from iceberg_migrate.cli import app as migrate_app

    runner = CliRunner()

    # Upload Spark scripts so Glue/EMR jobs can access them
    upload_spark_scripts(aws_s3_client)

    migrated: list[tuple[str, str, str]] = []
    synced_namespaces: list[str] = []

    try:
        for namespace in NAMESPACES:
            count = sync_minio_to_s3(minio_client, aws_s3_client, namespace)
            assert count > 0, (
                f"No objects synced for {namespace}. "
                f"Run: just seed-all"
            )
            synced_namespaces.append(namespace)

            src_prefix = f"s3://warehouse/{namespace}"
            dst_prefix = f"s3://{AWS_BUCKET}/warehouse/{namespace}"

            for table_name in TABLES_TO_MIGRATE:
                glue_table = f"{namespace}_{table_name}"
                table_location = (
                    f"s3://{AWS_BUCKET}/warehouse/{namespace}/{table_name}"
                )

                result = runner.invoke(
                    migrate_app,
                    [
                        "--table-location", table_location,
                        "--source-prefix", src_prefix,
                        "--dest-prefix", dst_prefix,
                        "--glue-database", GLUE_DB,
                        "--glue-table", glue_table,
                        "--aws-region", AWS_REGION,
                    ],
                )
                assert result.exit_code == 0, (
                    f"Migration failed for {namespace}.{table_name} "
                    f"(exit {result.exit_code}):\n{result.output}"
                )
                migrated.append((namespace, table_name, glue_table))

        yield migrated

    finally:
        for namespace in synced_namespaces:
            cleanup_s3_prefix(aws_s3_client, AWS_BUCKET, f"warehouse/{namespace}/")
        for _, _, glue_table in migrated:
            cleanup_glue_table(glue_client, GLUE_DB, glue_table)
```

- [ ] **Step 8: Commit**

```bash
git add tests/integration/conftest.py
git commit -m "feat(tests): add session migration fixture and Glue/EMR job helpers to conftest"
```

---

## Task 7: Refactor `test_athena_verify.py`

**Files:**
- Modify: `tests/integration/test_athena_verify.py`

Remove the per-test sync/migrate/cleanup. Use `migrated_tables` session fixture. Add the JOIN and cross-catalog queries.

- [ ] **Step 1: Rewrite `test_athena_verify.py`**

Replace the entire file with:

```python
"""AWS integration test: Athena verification of migrated Iceberg tables.

Requires:
  - Docker compose with all profiles up and seeded (just seed-all)
  - AWS credentials configured
  - Terraform infra applied (just tf-apply)
  - .env with AWS_TEST_BUCKET, GLUE_DATABASE, ATHENA_WORKGROUP

Migration is handled by the session-scoped `migrated_tables` fixture in conftest.py.
This file only exercises Athena queries.

Queries verified per namespace:
  1. Row count (SELECT COUNT(*))              → expect 10
  2. Dimension JOIN (sample_table ⋈ cities)   → expect 5 rows with region populated
  3. Cross-catalog JOIN (rest_ns ⋈ sql_ns)    → expect 3 rows with matching names
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.integration.conftest import (
    AWS_REGION,
    GLUE_DB,
    run_athena_query,
)

if TYPE_CHECKING:
    from mypy_boto3_athena import AthenaClient

CATALOG_CONFIGS = [
    pytest.param("rest_ns", "sample_table", id="rest"),
    pytest.param("sql_ns", "sample_table", id="sql"),
    pytest.param("hms_ns", "sample_table", id="hms"),
]


@pytest.fixture(scope="session")
def athena_client():
    import boto3
    return boto3.client("athena", region_name=AWS_REGION)


@pytest.mark.integration
@pytest.mark.parametrize("namespace,table_name", CATALOG_CONFIGS)
def test_athena_row_count(
    namespace: str,
    table_name: str,
    migrated_tables,
    athena_client: AthenaClient,
) -> None:
    """Athena COUNT(*) on migrated sample_table returns 10 seeded rows."""
    glue_table = f"{namespace}_{table_name}"
    rows = run_athena_query(
        athena_client,
        f"SELECT COUNT(*) AS cnt FROM {GLUE_DB}.{glue_table}",
    )
    assert len(rows) == 1
    assert int(rows[0][0]) == 10, f"Expected 10 rows, got {rows[0][0]}"


@pytest.mark.integration
@pytest.mark.parametrize("namespace,table_name", CATALOG_CONFIGS)
def test_athena_dimension_join(
    namespace: str,
    table_name: str,
    migrated_tables,
    athena_client: AthenaClient,
) -> None:
    """Athena JOIN of sample_table with cities returns 5 rows with region populated."""
    glue_table = f"{namespace}_{table_name}"
    cities_table = f"{namespace}_cities"
    rows = run_athena_query(
        athena_client,
        f"""
        SELECT s.name, c.region
        FROM {GLUE_DB}.{glue_table} s
        JOIN {GLUE_DB}.{cities_table} c ON s.city = c.city_name
        ORDER BY s.id
        LIMIT 5
        """,
    )
    assert len(rows) == 5, f"Expected 5 rows from dimension join, got {len(rows)}"
    regions = {row[1] for row in rows}
    assert regions <= {"Northern Vietnam", "Southern Vietnam", "Central Vietnam"}, (
        f"Unexpected region values: {regions}"
    )


@pytest.mark.integration
def test_athena_cross_catalog_join(
    migrated_tables,
    athena_client: AthenaClient,
) -> None:
    """Athena JOIN of rest_ns and sql_ns tables returns 3 rows with matching names."""
    rows = run_athena_query(
        athena_client,
        f"""
        SELECT r.id, r.name AS rest_name, s.name AS sql_name
        FROM {GLUE_DB}.rest_ns_sample_table r
        JOIN {GLUE_DB}.sql_ns_sample_table s ON r.id = s.id
        WHERE r.id <= 3
        ORDER BY r.id
        """,
    )
    assert len(rows) == 3, f"Expected 3 rows from cross-catalog join, got {len(rows)}"
    for row in rows:
        assert row[1] == row[2], (
            f"Names should match across catalogs: rest={row[1]}, sql={row[2]}"
        )
```

- [ ] **Step 2: Run Athena tests to verify they pass**

```bash
uv run pytest tests/integration/test_athena_verify.py -m integration -v
```

Expected: 7 tests pass (3 row count + 3 dimension join + 1 cross-catalog join).

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_athena_verify.py
git commit -m "refactor(tests): use session fixture + add JOIN queries in test_athena_verify"
```

---

## Task 8: Write `test_glue_verify.py`

**Files:**
- Create: `tests/integration/test_glue_verify.py`

- [ ] **Step 1: Create `tests/integration/test_glue_verify.py`**

```python
"""AWS integration test: Glue ETL verification of migrated Iceberg tables.

Requires:
  - Docker compose with all profiles up and seeded (just seed-all)
  - AWS credentials configured
  - Terraform infra applied (just tf-apply)
  - .env with AWS_TEST_BUCKET, GLUE_JOB_NAME

Submits verify_glue.py as a Glue ETL job run (Glue 4.0, G.1X × 2 workers).
Polls until SUCCEEDED, reads results.json from S3, asserts query outcomes.

Migration is handled by the session-scoped `migrated_tables` fixture in conftest.py.

Queries verified per namespace:
  1. Row count                   → expect 10
  2. Dimension JOIN (⋈ cities)   → expect 5 rows
  3. Cross-catalog JOIN          → expect 3 matching rows
"""

from __future__ import annotations

import uuid

import pytest

from tests.integration.conftest import (
    AWS_BUCKET,
    AWS_REGION,
    GLUE_DB,
    GLUE_JOB_NAME,
    cleanup_s3_prefix,
    read_job_results,
    run_glue_job,
)

CATALOG_CONFIGS = [
    pytest.param("rest_ns", id="rest"),
    pytest.param("sql_ns", id="sql"),
    pytest.param("hms_ns", id="hms"),
]


@pytest.fixture(scope="session")
def glue_job_client():
    import boto3
    return boto3.client("glue", region_name=AWS_REGION)


@pytest.mark.integration
@pytest.mark.parametrize("namespace", CATALOG_CONFIGS)
def test_glue_verifies_migrated_table(
    namespace: str,
    migrated_tables,
    glue_job_client,
    aws_s3_client,
) -> None:
    """Glue ETL job queries migrated Iceberg table and asserts row count, JOIN, and cross-catalog join."""
    run_id = uuid.uuid4().hex[:8]
    output_path = f"s3://{AWS_BUCKET}/integration-results/glue/{namespace}/{run_id}"

    try:
        run_glue_job(
            glue_job_client,
            GLUE_JOB_NAME,
            {
                "--output_path": output_path,
                "--glue_database": GLUE_DB,
                "--namespace": namespace,
                "--cross_ns1": "rest_ns",
                "--cross_ns2": "sql_ns",
            },
        )

        results = read_job_results(aws_s3_client, output_path)

        assert results["row_count"] == 10, (
            f"[{namespace}] Expected 10 rows, got {results['row_count']}"
        )
        assert len(results["join_rows"]) == 5, (
            f"[{namespace}] Expected 5 dimension join rows, got {len(results['join_rows'])}"
        )
        regions = {r["region"] for r in results["join_rows"]}
        assert regions <= {"Northern Vietnam", "Southern Vietnam", "Central Vietnam"}, (
            f"[{namespace}] Unexpected region values: {regions}"
        )
        assert len(results["cross_join_rows"]) == 3, (
            f"[{namespace}] Expected 3 cross-catalog join rows, got {len(results['cross_join_rows'])}"
        )
        for row in results["cross_join_rows"]:
            assert row["rest_name"] == row["sql_name"], (
                f"[{namespace}] Names should match: rest={row['rest_name']}, sql={row['sql_name']}"
            )

    finally:
        cleanup_s3_prefix(
            aws_s3_client,
            AWS_BUCKET,
            f"integration-results/glue/{namespace}/{run_id}/",
        )
```

- [ ] **Step 2: Run Glue tests**

```bash
uv run pytest tests/integration/test_glue_verify.py -m integration -v
```

Expected: 3 tests pass (one per namespace). Each test takes ~3–5 minutes for Glue job startup + execution.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_glue_verify.py
git commit -m "feat(tests): add Glue ETL integration test for migrated Iceberg tables"
```

---

## Task 9: Write `test_emr_verify.py`

**Files:**
- Create: `tests/integration/test_emr_verify.py`

- [ ] **Step 1: Create `tests/integration/test_emr_verify.py`**

```python
"""AWS integration test: EMR Serverless verification of migrated Iceberg tables.

Requires:
  - Docker compose with all profiles up and seeded (just seed-all)
  - AWS credentials configured
  - Terraform infra applied (just tf-apply)
  - .env with AWS_TEST_BUCKET, EMR_APPLICATION_ID, EMR_JOB_ROLE_ARN

Submits verify_emr.py as an EMR Serverless Spark job run (EMR 7.x).
Polls until SUCCESS, reads results.json from S3, asserts query outcomes.

Migration is handled by the session-scoped `migrated_tables` fixture in conftest.py.

Queries verified per namespace:
  1. Row count                   → expect 10
  2. Dimension JOIN (⋈ cities)   → expect 5 rows
  3. Cross-catalog JOIN          → expect 3 matching rows
"""

from __future__ import annotations

import uuid

import pytest

from tests.integration.conftest import (
    AWS_BUCKET,
    AWS_REGION,
    EMR_APPLICATION_ID,
    EMR_JOB_ROLE_ARN,
    GLUE_DB,
    cleanup_s3_prefix,
    read_job_results,
    run_emr_job,
)

CATALOG_CONFIGS = [
    pytest.param("rest_ns", id="rest"),
    pytest.param("sql_ns", id="sql"),
    pytest.param("hms_ns", id="hms"),
]

EMR_SCRIPT_S3_URI = f"s3://{AWS_BUCKET}/spark-jobs/verify_emr.py"


@pytest.fixture(scope="session")
def emrserverless_client():
    import boto3
    return boto3.client("emr-serverless", region_name=AWS_REGION)


@pytest.mark.integration
@pytest.mark.parametrize("namespace", CATALOG_CONFIGS)
def test_emr_verifies_migrated_table(
    namespace: str,
    migrated_tables,
    emrserverless_client,
    aws_s3_client,
) -> None:
    """EMR Serverless job queries migrated Iceberg table and asserts row count, JOIN, and cross-catalog join."""
    run_id = uuid.uuid4().hex[:8]
    output_path = f"s3://{AWS_BUCKET}/integration-results/emr/{namespace}/{run_id}"

    try:
        run_emr_job(
            emrserverless_client,
            EMR_APPLICATION_ID,
            EMR_JOB_ROLE_ARN,
            EMR_SCRIPT_S3_URI,
            entry_point_args=[
                "--output_path", output_path,
                "--glue_database", GLUE_DB,
                "--namespace", namespace,
                "--cross_ns1", "rest_ns",
                "--cross_ns2", "sql_ns",
            ],
        )

        results = read_job_results(aws_s3_client, output_path)

        assert results["row_count"] == 10, (
            f"[{namespace}] Expected 10 rows, got {results['row_count']}"
        )
        assert len(results["join_rows"]) == 5, (
            f"[{namespace}] Expected 5 dimension join rows, got {len(results['join_rows'])}"
        )
        regions = {r["region"] for r in results["join_rows"]}
        assert regions <= {"Northern Vietnam", "Southern Vietnam", "Central Vietnam"}, (
            f"[{namespace}] Unexpected region values: {regions}"
        )
        assert len(results["cross_join_rows"]) == 3, (
            f"[{namespace}] Expected 3 cross-catalog join rows, got {len(results['cross_join_rows'])}"
        )
        for row in results["cross_join_rows"]:
            assert row["rest_name"] == row["sql_name"], (
                f"[{namespace}] Names should match: rest={row['rest_name']}, sql={row['sql_name']}"
            )

    finally:
        cleanup_s3_prefix(
            aws_s3_client,
            AWS_BUCKET,
            f"integration-results/emr/{namespace}/{run_id}/",
        )
```

- [ ] **Step 2: Run EMR tests**

```bash
uv run pytest tests/integration/test_emr_verify.py -m integration -v
```

Expected: 3 tests pass. EMR Serverless job startup can take 2–5 minutes on first run; subsequent runs in the same session are faster as the application stays warm.

- [ ] **Step 3: Run the full integration suite to confirm all 3 engines pass together**

```bash
just test-integration
```

Expected: 13 tests pass total (7 Athena + 3 Glue + 3 EMR). Migration runs once (session fixture).

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_emr_verify.py
git commit -m "feat(tests): add EMR Serverless integration test for migrated Iceberg tables"
```

---

## Self-Review

**Spec coverage check:**
- ✅ `cities` table seeded (Task 1)
- ✅ IAM roles for Glue + EMR (Task 2)
- ✅ EMR Serverless application pre-provisioned (Task 2)
- ✅ Glue ETL job definition (Task 2)
- ✅ `.env.example` updated (Task 3)
- ✅ `verify_glue.py` Spark script (Task 4)
- ✅ `verify_emr.py` Spark script (Task 5)
- ✅ Session fixture + job helpers in conftest (Task 6)
- ✅ Athena refactored + JOIN queries added (Task 7)
- ✅ `test_glue_verify.py` (Task 8)
- ✅ `test_emr_verify.py` (Task 9)
- ✅ All 3 query scenarios verified by all 3 engines

**Type consistency:**
- `run_glue_job` returns `dict[str, Any]` — not used by callers (they only care about side effect + exception)
- `run_emr_job` returns `dict[str, Any]` — same
- `read_job_results` returns `dict[str, Any]` — callers index with `["row_count"]`, `["join_rows"]`, `["cross_join_rows"]` consistently across tasks 8 and 9
- `migrated_tables` yields `list[tuple[str, str, str]]` — not accessed by test bodies directly (they use `migrated_tables` as a marker that migration succeeded)
- `GLUE_JOB_NAME`, `EMR_APPLICATION_ID`, `EMR_JOB_ROLE_ARN` defined in conftest and imported in test files ✅
