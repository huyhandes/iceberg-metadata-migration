"""Seed an Iceberg table in MinIO via Lakekeeper REST catalog.

Requires: docker compose --profile rest up -d

Usage: uv run python infra/seed/seed_rest.py
"""

from __future__ import annotations

import os

import boto3
import requests
from botocore.client import Config
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO

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
    seed_table_with_history,
)

LAKEKEEPER_URI = os.environ.get("LAKEKEEPER_URI", "http://localhost:8181/catalog")
LAKEKEEPER_MGMT_URI = os.environ.get(
    "LAKEKEEPER_MGMT_URI", "http://localhost:8181/management"
)
WAREHOUSE_NAME = "warehouse"


DEFAULT_PROJECT_ID = "00000000-0000-0000-0000-000000000000"


def ensure_lakekeeper() -> None:
    """Create default project and register MinIO warehouse in Lakekeeper if not present."""
    # Ensure default project exists
    resp = requests.post(
        f"{LAKEKEEPER_MGMT_URI}/v1/project",
        json={"project-name": "default", "project-id": DEFAULT_PROJECT_ID},
    )
    if resp.status_code not in (201, 409):  # 409 = already exists
        resp.raise_for_status()

    # Check if warehouse already registered
    resp = requests.get(f"{LAKEKEEPER_MGMT_URI}/v1/warehouse")
    resp.raise_for_status()
    existing = {w["name"] for w in resp.json().get("warehouses", [])}
    if WAREHOUSE_NAME in existing:
        print(f"Warehouse '{WAREHOUSE_NAME}' already registered")
        return

    payload = {
        "warehouse-name": WAREHOUSE_NAME,
        "storage-profile": {
            "type": "s3",
            "bucket": "warehouse",
            "endpoint": MINIO_ENDPOINT,
            "region": MINIO_REGION,
            "path-style-access": True,
            "flavor": "minio",
            "sts-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": MINIO_ACCESS_KEY,
            "aws-secret-access-key": MINIO_SECRET_KEY,
        },
    }
    resp = requests.post(f"{LAKEKEEPER_MGMT_URI}/v1/warehouse", json=payload)
    resp.raise_for_status()
    print(f"Registered warehouse '{WAREHOUSE_NAME}' in Lakekeeper")


def main() -> None:
    ensure_lakekeeper()
    namespace = CATALOG_NAMESPACES["rest"]

    props = s3_properties()
    props["s3.path-style-access"] = "true"
    catalog = RestCatalog(
        "lakekeeper",
        uri=LAKEKEEPER_URI,
        warehouse=WAREHOUSE_NAME,  # Lakekeeper uses the registered name, not S3 URI
        **props,
    )

    # Create namespace if not exists
    try:
        catalog.create_namespace(namespace)
        print(f"Created namespace: {namespace}")
    except Exception:
        print(f"Namespace {namespace} already exists")

    table_id = (namespace, TABLE_NAME)

    # Drop catalog entry + purge S3 data for a clean slate
    try:
        catalog.drop_table(table_id)
        print(f"Dropped existing table: {namespace}.{TABLE_NAME}")
    except Exception:
        pass

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
        config=Config(s3={"addressing_style": "path"}),
    )
    prefix = f"{namespace}/{TABLE_NAME}/"
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket="warehouse", Prefix=prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket="warehouse", Delete={"Objects": objects})
            print(f"Purged {len(objects)} objects from s3://warehouse/{prefix}")

    # Create and populate
    table = catalog.create_table(
        identifier=table_id,
        schema=TABLE_SCHEMA,
        location=f"{WAREHOUSE}/{namespace}/{TABLE_NAME}",
    )
    # Lakekeeper uses S3V4RestSigner (remote signing) for writes, but PyIceberg's
    # FsspecFileIO only registers the signer on the sync boto3 client while s3fs uses
    # the async aiobotocore client — so signed headers are never sent and MinIO returns
    # 403. Bypass by replacing the table's FileIO with PyArrowFileIO which uses
    # credentials directly without going through remote signing.
    table.io = PyArrowFileIO(
        {
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.region": MINIO_REGION,
        }
    )
    seed_table_with_history(table)

    # Verify
    table = catalog.load_table(table_id)
    metadata = table.metadata
    print(f"Created {namespace}.{TABLE_NAME}")
    print(f"  Location: {metadata.location}")
    print(f"  Snapshots: {len(metadata.snapshots)}")
    print(f"  Metadata: {table.metadata_location}")

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


if __name__ == "__main__":
    main()
