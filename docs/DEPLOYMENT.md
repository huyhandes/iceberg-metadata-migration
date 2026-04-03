# Deployment

## Local Development Environment

### MinIO (S3-compatible local storage)

Docker Compose setup in `infra/docker-compose.yml`:

```bash
cd infra
docker compose up -d
```

This starts:
- **MinIO server** on `localhost:9000` (API) and `localhost:9001` (console)
- **minio-init** sidecar that creates the `warehouse` bucket

Credentials: `minioadmin` / `minioadmin`

### Seeding Test Data

```bash
cd infra
python seed_iceberg_table.py
```

Creates a sample Iceberg table in MinIO with metadata that can be migrated.

### Verifying a Migration

```bash
cd infra
./verify_migration.sh
```

### Copying to S3

```bash
cd infra
./copy_to_s3.sh
```

## AWS Infrastructure (Terraform)

Terraform config in `infra/terraform/main.tf`.

```bash
cd infra/terraform
terraform init
terraform plan
terraform apply
```

Provisions:
- S3 bucket for migrated data
- Glue database for table registration

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
