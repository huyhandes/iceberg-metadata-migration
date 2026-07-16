output "sandbox_lambda_function_name" {
  value       = aws_lambda_function.sandbox.function_name
  description = "Name of the sandbox Lambda function."
}

output "sandbox_ecr_repository_url" {
  value       = aws_ecr_repository.sandbox.repository_url
  description = "ECR repository URL for the sandbox container image."
}

output "sandbox_vpc_id" {
  value       = aws_vpc.sandbox.id
  description = "ID of the no-egress sandbox VPC."
}

output "sandbox_subnet_ids" {
  value       = [aws_subnet.sandbox_a.id, aws_subnet.sandbox_b.id]
  description = "IDs of both private subnets in the sandbox VPC."
}
