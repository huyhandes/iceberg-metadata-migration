terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    # Configured via: terraform init -backend-config=backend.tfvars
    # Required keys: bucket, key, region
  }
}

provider "aws" {
  region = var.aws_region
  # Set via AWS_PROFILE env var
}

# S3 bucket for Iceberg test data and Athena results
resource "aws_s3_bucket" "iceberg_test" {
  bucket = var.s3_bucket

  tags = {
    Project = "iceberg-migration-tool"
  }
}

resource "aws_s3_bucket_versioning" "iceberg_test" {
  bucket = aws_s3_bucket.iceberg_test.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Glue Catalog Database
resource "aws_glue_catalog_database" "iceberg_test" {
  name = "iceberg_migration_test"

  tags = {
    Project = "iceberg-migration-tool"
  }
}

# Athena workgroup for query verification
resource "aws_athena_workgroup" "iceberg_test" {
  name = "iceberg-migration-test"

  configuration {
    result_configuration {
      output_location = "s3://${var.s3_bucket}/athena-results/"
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    enforce_workgroup_configuration = false
  }

  tags = {
    Project = "iceberg-migration-tool"
  }
}

# Lake Formation permissions — grant IAM_ALLOWED_PRINCIPALS access to the database
# This ensures Athena can read Iceberg table columns without explicit LF grants
resource "aws_lakeformation_permissions" "database_all" {
  principal   = "IAM_ALLOWED_PRINCIPALS"
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.iceberg_test.name
  }
}

output "s3_bucket" {
  value = var.s3_bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.iceberg_test.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.iceberg_test.name
}

output "region" {
  value = var.aws_region
}

# ---------------------------------------------------------------------------
# Glue ETL job IAM role
# ---------------------------------------------------------------------------

resource "aws_iam_role" "glue_job_role" {
  name = "iceberg-migration-glue-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Project = "iceberg-migration-tool" }
}

resource "aws_iam_role_policy" "glue_job_policy" {
  name = "iceberg-migration-glue-job-policy"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}",
          "arn:aws:s3:::${var.s3_bucket}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions", "glue:GetTables"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["lakeformation:GetDataAccess"]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:*:*:/aws-glue/*"]
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# EMR Serverless IAM role
# ---------------------------------------------------------------------------

resource "aws_iam_role" "emr_serverless_role" {
  name = "iceberg-migration-emr-serverless-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Project = "iceberg-migration-tool" }
}

resource "aws_iam_role_policy" "emr_serverless_policy" {
  name = "iceberg-migration-emr-serverless-policy"
  role = aws_iam_role.emr_serverless_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}",
          "arn:aws:s3:::${var.s3_bucket}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions", "glue:GetTables"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["lakeformation:GetDataAccess"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"]
        Resource = ["*"]
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# EMR Serverless application (pre-provisioned — stays warm between test runs)
# ---------------------------------------------------------------------------

resource "aws_emrserverless_application" "iceberg_test" {
  name          = "iceberg-migration-test"
  release_label = var.emr_release_label
  type          = "SPARK"

  tags = { Project = "iceberg-migration-tool" }
}

# ---------------------------------------------------------------------------
# Glue ETL job definition
# ---------------------------------------------------------------------------

resource "aws_glue_job" "verify" {
  name     = "iceberg-migration-verify"
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket}/spark-jobs/verify_glue.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"            = "python"
    "--enable-glue-datacatalog" = "true"
    "--datalake-formats"        = "iceberg"
    "--enable-job-insights"     = "false"
  }

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 15

  tags = { Project = "iceberg-migration-tool" }
}

output "emr_application_id" {
  value = aws_emrserverless_application.iceberg_test.id
}

output "emr_job_role_arn" {
  value = aws_iam_role.emr_serverless_role.arn
}

output "glue_job_name" {
  value = aws_glue_job.verify.name
}
