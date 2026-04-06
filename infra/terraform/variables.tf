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
