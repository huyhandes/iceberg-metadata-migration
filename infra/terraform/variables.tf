variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
}

variable "s3_bucket" {
  description = "S3 bucket for test data and Athena results"
  type        = string
}
