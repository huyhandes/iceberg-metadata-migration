# ---------------------------------------------------------------------------
# Sandbox — no-egress VPC for the Lambda release gate test (ADR-0004)
#
# Deliberately omits: IGW, NAT gateway, and any route to 0.0.0.0/0.
# All outbound AWS API calls are routed through VPC endpoints only.
# The S3 bucket and Glue database are owned by the main stack (infra/terraform/)
# and only referenced here via var.s3_bucket / var.glue_database.
# ---------------------------------------------------------------------------

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_region" "current" {}

locals {
  tags = {
    Project = "iceberg-migration-sandbox"
    Env     = "sandbox"
  }
}

# ---------------------------------------------------------------------------
# VPC
# ---------------------------------------------------------------------------

resource "aws_vpc" "sandbox" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(local.tags, { Name = "iceberg-migration-sandbox" })
}

# ---------------------------------------------------------------------------
# Private subnets (no map_public_ip_on_launch — private by default)
# ---------------------------------------------------------------------------

resource "aws_subnet" "sandbox_a" {
  vpc_id            = aws_vpc.sandbox.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = merge(local.tags, { Name = "iceberg-migration-sandbox-a" })
}

resource "aws_subnet" "sandbox_b" {
  vpc_id            = aws_vpc.sandbox.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = merge(local.tags, { Name = "iceberg-migration-sandbox-b" })
}

# ---------------------------------------------------------------------------
# Route table — intentionally empty (no 0.0.0.0/0 route; no IGW; no NAT)
# ---------------------------------------------------------------------------

resource "aws_route_table" "sandbox" {
  vpc_id = aws_vpc.sandbox.id

  tags = merge(local.tags, { Name = "iceberg-migration-sandbox" })
}

resource "aws_route_table_association" "sandbox_a" {
  subnet_id      = aws_subnet.sandbox_a.id
  route_table_id = aws_route_table.sandbox.id
}

resource "aws_route_table_association" "sandbox_b" {
  subnet_id      = aws_subnet.sandbox_b.id
  route_table_id = aws_route_table.sandbox.id
}

# ---------------------------------------------------------------------------
# Security group — intra-VPC only (no egress to internet)
# ---------------------------------------------------------------------------

resource "aws_security_group" "sandbox" {
  name        = "iceberg-migration-sandbox"
  description = "Lambda and VPC endpoint SG — intra-VPC traffic only"
  vpc_id      = aws_vpc.sandbox.id

  ingress {
    description = "All traffic from within the VPC"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_vpc.sandbox.cidr_block]
  }

  egress {
    description = "All traffic to within the VPC only (no internet egress)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_vpc.sandbox.cidr_block]
  }

  tags = local.tags
}

# ---------------------------------------------------------------------------
# VPC Endpoints — required for Lambda to reach AWS services without internet
# ---------------------------------------------------------------------------

# S3 Gateway endpoint (free; routes S3 traffic through the AWS backbone)
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.sandbox.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.sandbox.id]

  tags = merge(local.tags, { Name = "iceberg-migration-sandbox-s3" })
}

# Glue Interface endpoint
resource "aws_vpc_endpoint" "glue" {
  vpc_id              = aws_vpc.sandbox.id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.glue"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.sandbox_a.id, aws_subnet.sandbox_b.id]
  security_group_ids  = [aws_security_group.sandbox.id]
  private_dns_enabled = true

  tags = merge(local.tags, { Name = "iceberg-migration-sandbox-glue" })
}

# ECR API Interface endpoint
resource "aws_vpc_endpoint" "ecr_api" {
  vpc_id              = aws_vpc.sandbox.id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.ecr.api"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.sandbox_a.id, aws_subnet.sandbox_b.id]
  security_group_ids  = [aws_security_group.sandbox.id]
  private_dns_enabled = true

  tags = merge(local.tags, { Name = "iceberg-migration-sandbox-ecr-api" })
}

# ECR DKR Interface endpoint (image layer pulls)
resource "aws_vpc_endpoint" "ecr_dkr" {
  vpc_id              = aws_vpc.sandbox.id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.ecr.dkr"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.sandbox_a.id, aws_subnet.sandbox_b.id]
  security_group_ids  = [aws_security_group.sandbox.id]
  private_dns_enabled = true

  tags = merge(local.tags, { Name = "iceberg-migration-sandbox-ecr-dkr" })
}

# CloudWatch Logs Interface endpoint
resource "aws_vpc_endpoint" "logs" {
  vpc_id              = aws_vpc.sandbox.id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.logs"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.sandbox_a.id, aws_subnet.sandbox_b.id]
  security_group_ids  = [aws_security_group.sandbox.id]
  private_dns_enabled = true

  tags = merge(local.tags, { Name = "iceberg-migration-sandbox-logs" })
}

# ---------------------------------------------------------------------------
# ECR repository
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "sandbox" {
  name                 = "iceberg-migration-sandbox"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  tags = local.tags
}

# ---------------------------------------------------------------------------
# IAM role for Lambda
# ---------------------------------------------------------------------------

resource "aws_iam_role" "sandbox_lambda" {
  name = "iceberg-migration-sandbox-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy" "sandbox_lambda" {
  name = "iceberg-migration-sandbox-policy"
  role = aws_iam_role.sandbox_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = ["arn:aws:s3:::${var.s3_bucket}"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["arn:aws:s3:::${var.s3_bucket}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["arn:aws:s3:::${var.s3_bucket}/*_migrated/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:CreateTable", "glue:GetTable", "glue:UpdateTable", "glue:GetDatabase"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:*:*:log-group:/aws/lambda/iceberg-migration-sandbox:*"]
      },
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"]
        Resource = [aws_ecr_repository.sandbox.arn]
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sandbox_vpc" {
  role       = aws_iam_role.sandbox_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# ---------------------------------------------------------------------------
# CloudWatch log group
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "sandbox" {
  name              = "/aws/lambda/iceberg-migration-sandbox"
  retention_in_days = 7

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Lambda function
# ---------------------------------------------------------------------------

resource "aws_lambda_function" "sandbox" {
  function_name = "iceberg-migration-sandbox"
  role          = aws_iam_role.sandbox_lambda.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.sandbox.repository_url}:${var.lambda_image_tag}"
  architectures = ["x86_64"]

  memory_size = var.lambda_memory_size
  timeout     = var.lambda_timeout

  vpc_config {
    subnet_ids         = [aws_subnet.sandbox_a.id, aws_subnet.sandbox_b.id]
    security_group_ids = [aws_security_group.sandbox.id]
  }

  depends_on = [aws_cloudwatch_log_group.sandbox]

  tags = local.tags
}
