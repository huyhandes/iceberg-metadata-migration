#!/usr/bin/env bash
# Copy Iceberg table from local MinIO to AWS S3 (simulates DataSync)
#
# Usage: ./infra/copy_to_s3.sh
#
# Requires:
#   - MinIO running (docker compose up)
#   - AWS CLI configured with profile ${AWS_PROFILE}
#   - S3 bucket YOUR_TEST_BUCKET exists

set -euo pipefail

MINIO_ENDPOINT="http://localhost:9000"
MINIO_BUCKET="warehouse"
MINIO_PREFIX="test_db/orders"

AWS_BUCKET="YOUR_TEST_BUCKET"
AWS_PREFIX="warehouse/test_db/orders"
AWS_PROFILE="${AWS_PROFILE}"
AWS_REGION="YOUR_REGION"

echo "=== Copying Iceberg table from MinIO to S3 ==="
echo "Source:  s3a://${MINIO_BUCKET}/${MINIO_PREFIX} (MinIO)"
echo "Dest:    s3://${AWS_BUCKET}/${AWS_PREFIX} (AWS S3)"
echo ""

# Use aws s3 sync with MinIO endpoint to list/download, then upload to real S3
# Step 1: Sync from MinIO to a local temp dir
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo "Step 1: Downloading from MinIO..."
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin aws s3 sync \
  "s3://${MINIO_BUCKET}/${MINIO_PREFIX}" \
  "${TMPDIR}/${MINIO_PREFIX}" \
  --endpoint-url "${MINIO_ENDPOINT}" \
  --region us-east-1 \
  2>&1

FILE_COUNT=$(find "$TMPDIR" -type f | wc -l | tr -d ' ')
echo "  Downloaded ${FILE_COUNT} files"

echo ""
echo "Step 2: Uploading to AWS S3..."
aws s3 sync \
  "${TMPDIR}/${MINIO_PREFIX}" \
  "s3://${AWS_BUCKET}/${AWS_PREFIX}" \
  --profile "${AWS_PROFILE}" \
  --region "${AWS_REGION}" \
  2>&1

echo ""
echo "Step 3: Verifying upload..."
UPLOADED=$(aws s3 ls "s3://${AWS_BUCKET}/${AWS_PREFIX}/" \
  --recursive \
  --profile "${AWS_PROFILE}" \
  --region "${AWS_REGION}" \
  | wc -l | tr -d ' ')
echo "  ${UPLOADED} files in S3"

echo ""
echo "=== Copy complete ==="
echo ""
echo "Files are in S3 but metadata still references MinIO paths (s3a://warehouse/...)."
echo "Run the migration tool to fix paths:"
echo ""
echo "  uv run iceberg-migrate \\"
echo "    --source-prefix s3a://warehouse \\"
echo "    --dest-prefix s3://${AWS_BUCKET}/warehouse \\"
echo "    --table-location s3://${AWS_BUCKET}/${AWS_PREFIX} \\"
echo "    --glue-database iceberg_migration_test \\"
echo "    --aws-region ${AWS_REGION}"
