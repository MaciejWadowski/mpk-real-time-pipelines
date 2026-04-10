output "iam_user_arn" {
  description = "ARN of the created IAM user"
  value       = aws_iam_user.this.arn
}

output "iam_user_name" {
  description = "Name of the created IAM user"
  value       = aws_iam_user.this.name
}

output "iam_user_unique_id" {
  description = "Unique ID assigned by AWS to the IAM user"
  value       = aws_iam_user.this.unique_id
}

output "attached_policy_arns" {
  description = "ARNs of all policies attached to the user"
  value = [
    "arn:aws:iam::aws:policy/ReadOnlyAccess",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    aws_iam_policy.rds_small_instances.arn,
    aws_iam_policy.ec2_small_instances.arn,
  ]
}

output "access_key_id" {
  description = "Access key ID (only populated when create_access_key = true)"
  value       = var.create_access_key ? aws_iam_access_key.this[0].id : null
}

output "access_key_secret" {
  description = "Secret access key (only populated when create_access_key = true)"
  value       = var.create_access_key ? aws_iam_access_key.this[0].secret : null
  sensitive   = true
}

output "console_password" {
  description = "One-time password for first console login (sensitive)"
  value       = aws_iam_user_login_profile.this.password
  sensitive   = true
}