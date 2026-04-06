# Deployment

## Local Development Environment

### MinIO (S3-compatible local storage)

Docker Compose setup in `infra/docker-compose.yml`:

```bash
just infra-up
```

This starts MinIO on `localhost:9000` (API) and `localhost:9001` (console), plus catalog services depending on active profiles. See [INFRASTRUCTURE.md](INFRASTRUCTURE.md) for the full service list.

Credentials: `minioadmin` / `minioadmin`

### Seeding Test Data

```bash
just seed-all          # Seed all catalog types (REST, SQL, HMS)
just seed-rest         # Lakekeeper REST catalog only
just seed-sql          # SQLite SQL catalog only
just seed-hms          # Hive Metastore only
```

See [INFRASTRUCTURE.md](INFRASTRUCTURE.md) for seed script details.

## AWS Infrastructure (Terraform)

Terraform config in `infra/terraform/main.tf`.

```bash
just tf-init     # Initialize backend
just tf-plan     # Preview changes
just tf-apply    # Apply changes
just tf-destroy  # Tear down (careful!)
```

Provisions:
- S3 bucket for test data, Athena results, and Spark scripts
- Glue database for table registration
- Athena workgroup (engine v3) for query verification
- Lake Formation database-level permissions for IAM_ALLOWED_PRINCIPALS
- Glue ETL job definition (`iceberg-migration-verify`, Glue 5.1)
- IAM role for the Glue ETL job
- EMR Serverless application (`iceberg-migration-test`, EMR 7.12.0)
- IAM role for EMR Serverless jobs

After `tf-apply`, copy the outputs (`emr_application_id`, `emr_job_role_arn`, `glue_job_name`) into `.env`.

### Verifying the Integration

With infrastructure provisioned and all catalogs seeded:

```bash
just test-integration   # All engines (Athena + Glue ETL + EMR Serverless)
just test-athena        # Athena only
just test-glue          # Glue ETL only
just test-emr           # EMR Serverless only
```

## Running the Tool

### From source (development):
```bash
uv run iceberg-migrate migrate \
  --table-location s3://bucket/warehouse/db/table \
  --source-prefix s3a://old-bucket/warehouse \
  --dest-prefix s3://bucket/warehouse
```

### Installed:
```bash
pip install .
iceberg-migrate migrate \
  --table-location s3://bucket/warehouse/db/table \
  --source-prefix s3a://old-bucket/warehouse \
  --dest-prefix s3://bucket/warehouse
```

## AWS Credentials

The tool uses the default boto3 credential chain:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS config files (`~/.aws/credentials`, `~/.aws/config`)
3. IAM role (EC2/ECS/Lambda)

Required permissions:
- `s3:GetObject` on source metadata files
- `s3:PutObject` on `_migrated/` prefix
- `glue:CreateTable`, `glue:GetTable`, `glue:UpdateTable` on target database
