# AWS Lambda Entry Point

The Lambda is the second **Entry point** alongside the CLI. Airflow invokes it
synchronously (`RequestResponse`) over a PrivateLink endpoint with a JSON payload
that mirrors the CLI arguments 1:1 in snake_case. The handler is a thin adapter:
it maps the event to a `MigrationParams`, calls the shared `run_migration()`
(ADR-0001 — same function the CLI calls), and returns the summary as a dict. The
pipeline modules are unchanged.

> The CLI remains the other Entry point. Both share one orchestration path; see
> [CLI.md](CLI.md) for the flag equivalents and [DEPLOYMENT.md](DEPLOYMENT.md)
> for the host path.

## Event Contract

A JSON object whose keys are the CLI option names in snake_case. Missing a
required key makes the handler raise before any work.

| Key | Required | Default | Notes |
|-----|----------|---------|-------|
| `table_location` | yes | — | S3 URI of the Iceberg table root |
| `source_prefix` | yes | — | The stale **Source prefix** to Rewrite |
| `dest_prefix` | yes | — | The **Destination prefix** to point metadata at |
| `glue_database` | no | derived from table location | Override Glue database |
| `glue_table` | no | derived from table location | Override Glue table |
| `dry_run` | no | `false` | Report what would be written; no S3 writes, no **Registration** |
| `verbose` | no | `false` | Per-file Rewrite counts in logs |
| `aws_region` | no | env `AWS_DEFAULT_REGION` | Region for Glue; falls back to the Lambda environment |

## Response and Failure Contract

- **Success** returns a JSON object with the same shape as the CLI's `--json`
  (`render_json`): `status: "success"`, the rewritten-file counts, and the Glue
  **Registration** outcome. Lambda serializes the returned dict.
- **Any failure — partial or fatal — raises** (ADR-0002). Lambda reports a
  `FunctionError`. The caller (Airflow) treats `FunctionError` as a failed task
  and retries. A retry is safe because the tool is non-destructive and
  idempotent: **Migrated metadata** is written under `_migrated/` and originals
  are never touched. Partial writes are not rolled back; a retry re-runs the
  whole Invocation.

This collapses the CLI's 0/1/2 exit codes into return-vs-raise, because an
orchestrator only needs task-succeeded vs. task-failed.

## Example Invocation Payload

Minimal required:

```json
{
  "table_location": "s3://my-bucket/warehouse/analytics/events",
  "source_prefix": "s3a://old-bucket/warehouse",
  "dest_prefix": "s3://my-bucket/warehouse"
}
```

Full payload (mirrors the CLI examples in [DEPLOYMENT.md](DEPLOYMENT.md)):

```json
{
  "table_location": "s3://my-bucket/warehouse/analytics/events",
  "source_prefix": "hdfs://namenode:8020/warehouse",
  "dest_prefix": "s3://my-bucket/warehouse",
  "glue_database": "my_analytics",
  "glue_table": "user_events",
  "dry_run": false,
  "verbose": true,
  "aws_region": "us-east-1"
}
```

## IAM Permissions (Least Privilege)

Scope the execution role to:

- `s3:GetObject` on the source metadata files (manifests, `metadata.json`)
- `s3:ListBucket` on the table bucket (metadata discovery)
- `s3:PutObject` under `_migrated/` only
- `glue:CreateTable`, `glue:GetTable`, `glue:UpdateTable`, `glue:GetDatabase` on the target database
- `logs:CreateLogStream`, `logs:PutLogEvents` on the function's log group
- The AWS-managed `AWSLambdaVPCAccessExecutionRole` policy (required for VPC attachment — grants ENI create/describe)

## VPC and Endpoint Prerequisites

The production function runs in private subnets with **no internet egress**
(ADR-0004). It reaches AWS over VPC endpoints that **already exist in the client
VPC** and are **not** created by this repo:

- **S3** — Gateway VPC endpoint (the function reads source metadata and writes
  **Migrated metadata**)
- **Glue** — Interface VPC endpoint (for **Registration**)
- **Lambda invoke path** — a separate PrivateLink interface endpoint for
  Airflow → Lambda control plane. This is independent of whether the function is
  attached to a VPC.

The function code and image are identical regardless of VPC attachment. These
endpoints are client-owned; this repo's Terraform provisions only a test Lambda.

## Environment Variables

- `AWS_DEFAULT_REGION` — the region fallback when `aws_region` is omitted from the
  payload.

The function runs under its execution role for AWS credentials — no static access
keys are configured.

## Image Build and Push

The image is `FROM public.ecr.aws/lambda/python:3.12`, x86_64, with dependencies
installed from a `uv export`-generated `requirements.txt` for reproducibility
against `uv.lock` (ADR-0003). The `Dockerfile` at the repo root sets
`CMD ["iceberg_migrate.lambda_handler.handler"]`.

```bash
just lambda-build
# equivalent to:
# docker build --platform linux/amd64 -t iceberg-migrate-lambda:latest .
```

Push to the ECR repo (requires `AWS_ACCOUNT_ID` and `AWS_REGION` in `.env`, and
the ECR repo from `just tf-apply`):

```bash
just lambda-push
```

## Repository Terraform Scope

This repo's Terraform provisions a **test Lambda only** (ADR-0004), in
`infra/terraform/lambda.tf`: an ECR repository, an `aws_lambda_function` from the
image (x86_64, memory 1024 MB, timeout 300 s — both variables), the
least-privilege IAM role and policy above, a CloudWatch log group, and optional
`vpc_config` from `lambda_subnet_ids` + `lambda_security_group_id` variables. It
does **not** create the S3/Glue VPC endpoints, the PrivateLink invoke endpoint,
or the production function — production deployment and networking are owned by
the client's platform team.
