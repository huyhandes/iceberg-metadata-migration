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
from pyspark.sql.functions import broadcast, col

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser(description="EMR Serverless Iceberg validation")
parser.add_argument("--output_path", required=True)
parser.add_argument("--glue_database", required=True)
parser.add_argument("--namespace", required=True)
parser.add_argument("--cross_ns1", required=True)
parser.add_argument("--cross_ns2", required=True)
parser.add_argument("--aws_region", required=True)
parser.add_argument("--s1_timestamp", required=True)
parser.add_argument("--s2_timestamp", required=True)
args = parser.parse_args()

db = args.glue_database
ns = args.namespace
cross_ns1 = args.cross_ns1
cross_ns2 = args.cross_ns2
aws_region = args.aws_region
output_path = args.output_path.rstrip("/")
s1_timestamp = args.s1_timestamp
s2_timestamp = args.s2_timestamp

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
    boto3.client("s3", region_name=aws_region).put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(results).encode(),
    )


# ---------------------------------------------------------------------------
# Load tables lazily — no action triggered until first count/iterator
# ---------------------------------------------------------------------------

sample_df = spark.table(f"glue_catalog.`{db}`.`{ns}_sample_table`")
cities_df = spark.table(f"glue_catalog.`{db}`.`{ns}_cities`")

# ---------------------------------------------------------------------------
# Query 1: Row count — df.count() avoids collecting a result set
# ---------------------------------------------------------------------------

row_count = sample_df.count()

# ---------------------------------------------------------------------------
# Query 2: Dimension JOIN — broadcast cities (3 rows), stream results lazily
# ---------------------------------------------------------------------------

join_df = (
    sample_df.join(broadcast(cities_df), sample_df["city"] == cities_df["city_name"])
    .select(sample_df["name"], cities_df["region"])
    .orderBy(sample_df["id"])
    .limit(5)
)
join_results = [
    {"name": r["name"], "region": r["region"]} for r in join_df.toLocalIterator()
]

# ---------------------------------------------------------------------------
# Query 3: Cross-catalog JOIN — filter before join to minimise shuffle,
#           broadcast the filtered side (≤3 rows)
# ---------------------------------------------------------------------------

rest_df = spark.table(f"glue_catalog.`{db}`.`{cross_ns1}_sample_table`").filter(
    col("id") <= 3
)
sql_df = spark.table(f"glue_catalog.`{db}`.`{cross_ns2}_sample_table`").filter(
    col("id") <= 3
)
cross_df = (
    rest_df.join(broadcast(sql_df), "id")
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
# Query 4: Time-travel to snapshot 1 — row count and SUM(amount)
# ---------------------------------------------------------------------------

s1_table_name = f"glue_catalog.`{db}`.`{ns}_sample_table`"
s1_df = spark.read.option("as-of-timestamp", s1_timestamp).table(s1_table_name)
s1_row_count = s1_df.count()
s1_sum_row = s1_df.agg({"amount": "sum"}).collect()
s1_sum_amount = float(s1_sum_row[0][0]) if s1_sum_row else 0.0

# ---------------------------------------------------------------------------
# Query 5: Time-travel to snapshot 2 — row count and SUM(amount)
# ---------------------------------------------------------------------------

s2_df = spark.read.option("as-of-timestamp", s2_timestamp).table(s1_table_name)
s2_row_count = s2_df.count()
s2_sum_row = s2_df.agg({"amount": "sum"}).collect()
s2_sum_amount = float(s2_sum_row[0][0]) if s2_sum_row else 0.0

# ---------------------------------------------------------------------------
# Write results
# ---------------------------------------------------------------------------

results = {
    "row_count": row_count,
    "join_rows": join_results,
    "cross_join_rows": cross_results,
    "s1_row_count": s1_row_count,
    "s1_sum_amount": s1_sum_amount,
    "s2_row_count": s2_row_count,
    "s2_sum_amount": s2_sum_amount,
}
put_results(output_path, results)

spark.stop()
