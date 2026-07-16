# ---------------------------------------------------------------------------
# TEST-ONLY Lambda (ADR-0004)
#
# Provisions a single test Lambda built from the migration container image,
# mirroring the existing Glue-ETL / EMR / Athena test resources in main.tf.
# It deliberately does NOT create the S3/Glue VPC endpoints or the production
# function — production networking and deployment are owned by the client's
# platform team and documented (docs/DEPLOYMENT.md / docs/LAMBDA.md), not
# codified here, so this Terraform never conflicts with client-managed
# networking.
# ---------------------------------------------------------------------------

# ECR repository for the migration container image (force_delete for test cleanup)
resource "aws_ecr_repository" "lambda" {
  name                 = "iceberg-migration-lambda"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  tags = { Project = "iceberg-migration-tool" }
}

# IAM role for the Lambda
resource "aws_iam_role" "lambda" {
  name = "iceberg-migration-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Project = "iceberg-migration-tool" }
}

# Least-privilege policy: read source metadata, write under _migrated/, manage the target Glue table.
resource "aws_iam_role_policy" "lambda" {
  name = "iceberg-migration-lambda-policy"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # List the test bucket so the function can discover metadata objects.
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = ["arn:aws:s3:::${var.s3_bucket}"]
      },
      {
        # Read source Iceberg metadata (manifests, metadata.json, etc.).
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["arn:aws:s3:::${var.s3_bucket}/*"]
      },
      {
        # Write Migrated metadata only under _migrated/. Scoping the prefix via
        # the resource ARN wildcard is the standard least-priv pattern; the
        # migration tool writes _migrated/ as a leaf segment anywhere in the
        # bucket, hence *_migrated/*. Originals are never touched.
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["arn:aws:s3:::${var.s3_bucket}/*_migrated/*"]
      },
      {
        # Create / read / update the target Glue table.
        Effect   = "Allow"
        Action   = ["glue:CreateTable", "glue:GetTable", "glue:UpdateTable", "glue:GetDatabase"]
        Resource = ["*"]
      },
      {
        # CloudWatch Logs scoped to this function's log group.
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:*:*:log-group:/aws/lambda/iceberg-migration-lambda:*"]
      },
    ]
  })
}

# CloudWatch log group (created here so retention is controlled explicitly)
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/iceberg-migration-lambda"
  retention_in_days = 14

  tags = { Project = "iceberg-migration-tool" }
}

# Lambda function from the ECR-hosted container image.
resource "aws_lambda_function" "lambda" {
  function_name = "iceberg-migration-lambda"
  role          = aws_iam_role.lambda.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.lambda.repository_url}:${var.lambda_image_tag}"
  architectures = ["x86_64"]

  memory_size = var.lambda_memory_size
  timeout     = var.lambda_timeout

  # VPC attachment is optional: when lambda_subnet_ids is empty the function
  # runs outside a VPC (local/test invocations); set the vars to attach it to
  # private subnets per ADR-0004. Production networking is client-owned.
  dynamic "vpc_config" {
    for_each = length(var.lambda_subnet_ids) > 0 ? [1] : []
    content {
      subnet_ids         = var.lambda_subnet_ids
      security_group_ids = [var.lambda_security_group_id]
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda]

  tags = { Project = "iceberg-migration-tool" }
}

output "lambda_function_name" {
  value = aws_lambda_function.lambda.function_name
}

output "lambda_function_arn" {
  value = aws_lambda_function.lambda.arn
}

output "ecr_repository_url" {
  value = aws_ecr_repository.lambda.repository_url
}
