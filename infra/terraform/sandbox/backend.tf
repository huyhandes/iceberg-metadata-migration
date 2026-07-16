terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {
    # Configured via: terraform init -backend-config=backend.tfvars
    # Required keys: bucket, key, region
    # key = "terraform/iceberg-migration-sandbox.tfstate"
  }
}

provider "aws" {
  region = var.aws_region
}
