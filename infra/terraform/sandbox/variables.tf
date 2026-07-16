variable "aws_region" {
  type        = string
  description = "AWS region to deploy sandbox resources into."
}

variable "s3_bucket" {
  type        = string
  description = "Name of the repo-managed test S3 bucket. Not created here — must already exist."
}

variable "glue_database" {
  type        = string
  default     = "iceberg_migration_test"
  description = "Existing Glue database name. Not created here — must already exist."
}

variable "lambda_image_tag" {
  type        = string
  default     = "latest"
  description = "Container image tag to deploy from the sandbox ECR repository."
}

variable "lambda_memory_size" {
  type        = number
  default     = 1024
  description = "Memory (MB) allocated to the sandbox Lambda."
}

variable "lambda_timeout" {
  type        = number
  default     = 300
  description = "Timeout (seconds) for the sandbox Lambda."
}
