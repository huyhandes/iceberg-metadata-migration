variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
}

variable "s3_bucket" {
  description = "S3 bucket for test data and Athena results"
  type        = string
}

variable "emr_release_label" {
  description = "EMR release label for the Serverless Spark application"
  type        = string
  default     = "emr-7.12.0"
}

# ---------------------------------------------------------------------------
# Test Lambda (ADR-0004) — see lambda.tf
# ---------------------------------------------------------------------------

variable "lambda_memory_size" {
  description = "Memory (MB) for the test Lambda function"
  type        = number
  default     = 1024
}

variable "lambda_timeout" {
  description = "Timeout (seconds) for the test Lambda function"
  type        = number
  default     = 300
}

variable "lambda_subnet_ids" {
  description = "Private subnet IDs for VPC attachment. Empty = no VPC (local/test)."
  type        = list(string)
  default     = []
}

variable "lambda_security_group_id" {
  description = "Security group ID for the VPC-attached function. Used only when lambda_subnet_ids is set."
  type        = string
  default     = ""
}

variable "lambda_image_tag" {
  description = "ECR image tag deployed by the test Lambda"
  type        = string
  default     = "latest"
}
