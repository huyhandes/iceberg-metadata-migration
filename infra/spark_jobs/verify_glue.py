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
  Tables are referenced as {db}.{table} (2-part) — --datalake-formats iceberg
  configures Glue Data Catalog as spark_catalog (the default Spark catalog),
  so no catalog prefix is needed.
"""

from __future__ import annotations

import json
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import broadcast, col

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
# Load tables lazily — no action triggered until first count/iterator
# ---------------------------------------------------------------------------

sample_df = spark.table(f"`{db}`.`{ns}_sample_table`")
cities_df = spark.table(f"`{db}`.`{ns}_cities`")

# ---------------------------------------------------------------------------
# Query 1: Row count — df.count() avoids collecting a result set
# ---------------------------------------------------------------------------

row_count = sample_df.count()

# ---------------------------------------------------------------------------
# Query 2: Dimension JOIN — broadcast cities (3 rows), stream results lazily
# ---------------------------------------------------------------------------

join_df = (
    sample_df
    .join(broadcast(cities_df), sample_df["city"] == cities_df["city_name"])
    .select(sample_df["name"], cities_df["region"])
    .orderBy(sample_df["id"])
    .limit(5)
)
join_results = [
    {"name": r["name"], "region": r["region"]}
    for r in join_df.toLocalIterator()
]

# ---------------------------------------------------------------------------
# Query 3: Cross-catalog JOIN — filter before join to minimise shuffle,
#           broadcast the filtered side (≤3 rows)
# ---------------------------------------------------------------------------

rest_df = spark.table(f"`{db}`.`{cross_ns1}_sample_table`").filter(col("id") <= 3)
sql_df = spark.table(f"`{db}`.`{cross_ns2}_sample_table`").filter(col("id") <= 3)
cross_df = (
    rest_df
    .join(broadcast(sql_df), "id")
    .select(
        rest_df["id"],
        rest_df["name"].alias("rest_name"),
        sql_df["name"].alias("sql_name"),
    )
    .orderBy("id")
)
cross_results = [
    {"id": r["id"], "rest_name": r["rest_name"], "sql_name": r["sql_name"]}
    for r in cross_df.toLocalIterator()
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
