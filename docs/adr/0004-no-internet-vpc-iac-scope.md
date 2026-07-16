# No-internet VPC function; repo Terraform is test-only (production networking is client-owned)

The production function runs in **private subnets with no internet egress**, reaching S3 via
a Gateway VPC endpoint and Glue via an Interface VPC endpoint that **already exist** in the
client's VPC.

This repo's Terraform provisions:

1. **Main stack** (`infra/terraform/`) — a test Lambda (ECR repo, function-from-image, IAM
   role, log group, optional `vpc_config` variables). Does **not** create the S3/Glue VPC
   endpoints or the production function. Production deployment and networking are owned by
   the client's platform team.

2. **Sandbox stack** (`infra/terraform/sandbox/`) — a destroyable, isolated sandbox VPC
   (private subnets only, no IGW, no NAT) with S3/Glue/ECR/Logs VPC endpoints and a
   test Lambda (`iceberg-migration-sandbox`). This stack exists solely to run the no-egress
   release gate (`just test-lambda-release`) and is fully destroyed after the run. It
   reuses the repo-managed test S3 bucket and Glue database; it creates no resource
   pre-existing on the account.

## Scope of the ban on production networking

The prohibition on client-managed networking (the ADR's original rationale — avoid
conflicting with the client's platform team) applies to **production networking only**:
VPC endpoints in the client's VPC, the production Lambda function, and the PrivateLink
invoke endpoint. It does **not** apply to a throwaway sandbox VPC that:

- Is fully owned and destroyed by this repo's CI/CD and release workflow.
- Has no connection to, and cannot conflict with, any client-managed resource.
- Exists solely to verify the no-egress networking requirement before shipping.

## Consequences

Required IAM permissions, VPC wiring, and endpoint prerequisites for the **production**
function live in documentation (`docs/DEPLOYMENT.md` / `docs/LAMBDA.md`), not in the
main IaC — the boundary is intentional so the repo's Terraform never conflicts with
client-managed networking. The sandbox stack is explicitly exempt from this boundary.
