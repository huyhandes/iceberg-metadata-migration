#!/usr/bin/env bash
# Verify Iceberg migration worked by querying with Athena
#
# Usage: ./infra/verify_migration.sh
#
# Requires:
#   - Migration tool has been run
#   - Glue table registered
#   - Athena workgroup exists

set -euo pipefail

AWS_PROFILE="${AWS_PROFILE}"
AWS_REGION="YOUR_REGION"
GLUE_DB="iceberg_migration_test"
TABLE="orders"
WORKGROUP="iceberg-migration-test"

echo "=== Verifying Migration via Athena ==="
echo ""

# Run a simple query
QUERY="SELECT * FROM ${GLUE_DB}.${TABLE} LIMIT 10"
echo "Query: ${QUERY}"
echo ""

EXECUTION_ID=$(aws athena start-query-execution \
  --query-string "${QUERY}" \
  --work-group "${WORKGROUP}" \
  --query-execution-context "Database=${GLUE_DB}" \
  --profile "${AWS_PROFILE}" \
  --region "${AWS_REGION}" \
  --output text --query "QueryExecutionId")

echo "Execution ID: ${EXECUTION_ID}"
echo "Waiting for results..."

# Poll for completion
for i in $(seq 1 30); do
  STATUS=$(aws athena get-query-execution \
    --query-execution-id "${EXECUTION_ID}" \
    --profile "${AWS_PROFILE}" \
    --region "${AWS_REGION}" \
    --output text --query "QueryExecution.Status.State")

  if [ "$STATUS" = "SUCCEEDED" ]; then
    echo "Query SUCCEEDED!"
    echo ""
    aws athena get-query-results \
      --query-execution-id "${EXECUTION_ID}" \
      --profile "${AWS_PROFILE}" \
      --region "${AWS_REGION}" \
      --output table
    echo ""
    echo "=== Migration verified! Athena can query the migrated table. ==="
    exit 0
  elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
    echo "Query ${STATUS}!"
    aws athena get-query-execution \
      --query-execution-id "${EXECUTION_ID}" \
      --profile "${AWS_PROFILE}" \
      --region "${AWS_REGION}" \
      --output json --query "QueryExecution.Status"
    exit 1
  fi

  sleep 2
done

echo "Timeout waiting for query results"
exit 1
