# No-internet VPC function; repo Terraform is test-only

The production function runs in **private subnets with no internet egress**, reaching S3 via
a Gateway VPC endpoint and Glue via an Interface VPC endpoint that **already exist** in the
client's VPC. Reaching the Lambda control plane is likewise private (Airflow invokes over a
PrivateLink interface endpoint) — that is separate from the function's own VPC attachment.

This repo's Terraform provisions a **test Lambda only** (ECR repo, function-from-image, IAM
role with least-privilege S3/Glue access, log group, `vpc_config` from variables), matching
the existing pattern where the repo provisions test Glue-ETL / EMR / Athena purely to verify
migrations. It deliberately does **not** create the S3/Glue VPC endpoints or the production
function. Production deployment and networking are owned by the client's platform team and
are documented (`docs/DEPLOYMENT.md` / `docs/LAMBDA.md`), not codified here.

## Consequences

The required IAM permissions, VPC wiring, and endpoint prerequisites live in documentation,
not in this repo's IaC — the boundary is intentional so the repo's Terraform never conflicts
with client-managed networking.
