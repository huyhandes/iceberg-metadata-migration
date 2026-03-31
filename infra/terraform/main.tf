terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket  = "YOUR_TEST_BUCKET"
    key     = "terraform/iceberg-migration.tfstate"
    region  = "YOUR_REGION"
    profile = "${AWS_PROFILE}"
  }
}

provider "aws" {
  region  = "YOUR_REGION"
  profile = "${AWS_PROFILE}"
}

# S3 bucket created manually (also used as terraform backend)
# aws s3 mb s3://YOUR_TEST_BUCKET --region YOUR_REGION --profile ${AWS_PROFILE}

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
      output_location = "s3://YOUR_TEST_BUCKET/athena-results/"
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
  value = "YOUR_TEST_BUCKET"
}

output "glue_database" {
  value = aws_glue_catalog_database.iceberg_test.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.iceberg_test.name
}

output "region" {
  value = "YOUR_REGION"
}
