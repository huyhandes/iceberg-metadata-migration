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

| Resource | Name | Purpose |
|----------|------|---------|
| Glue Database | `iceberg_migration_test` | Target catalog for migrated tables |
| Athena Workgroup | `iceberg-migration-test` | Query engine (v3) for verification |
| Lake Formation | IAM_ALLOWED_PRINCIPALS | Athena column-level access |

### Usage

```bash
just tf-init     # Initialize backend
just tf-plan     # Preview changes
just tf-apply    # Apply changes
just tf-destroy  # Tear down (careful!)
```

All `just tf-*` commands inject `AWS_PROFILE=YOUR_AWS_PROFILE` automatically.

## Seed Scripts

Located at `infra/seed/`. Create real Iceberg tables through different catalogs into MinIO.

| Script | Catalog | Namespace | Docker Profile |
|--------|---------|-----------|---------------|
| `seed_rest.py` | Lakekeeper REST | `rest_ns` | `rest` |
| `seed_sql.py` | SQLite SQL | `sql_ns` | (none) |
| `seed_hms.py` | Hive Metastore | `hms_ns` | `hms` |

Each writes to `s3://warehouse/{namespace}/sample_table/` on MinIO. Namespaces are isolated so all catalogs coexist without conflicts.

### AWS Documentation References

Official AWS docs supporting Athena as the verification layer:

- [Querying Iceberg tables in Athena](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
- [Glue Catalog as Iceberg catalog](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-glue-catalog.html)
- [Glue CreateTable API](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html)
- [Athena engine versions](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference.html)

A successful Athena `SELECT` validates the entire migration chain: Glue registration -> metadata.json -> Avro manifests -> data file accessibility.
