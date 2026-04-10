variable "aws_region" {
  description = "AWS region for the provider"
  type        = string
  default     = "us-east-1"
}

variable "iam_username" {
  description = "Name of the IAM user to create"
  type        = string
  default     = "limited-power-user"
}

variable "create_access_key" {
  description = "Whether to create programmatic access keys for the user"
  type        = bool
  default     = false
}

variable "allowed_ec2_instance_types" {
  description = "List of allowed EC2 instance types. Only nano/micro/small burstable families are permitted by default."
  type        = list(string)
  default = [
    "t2.nano", "t2.micro", "t2.small",
    "t3.nano", "t3.micro", "t3.small",
    "t3a.nano", "t3a.micro", "t3a.small",
    "t4g.nano", "t4g.micro", "t4g.small",
  ]
}

variable "allowed_rds_instance_classes" {
  description = "List of allowed RDS DB instance classes. Only small burstable classes are permitted by default."
  type        = list(string)
  default = [
    "db.t2.micro", "db.t2.small",
    "db.t3.micro", "db.t3.small",
    "db.t3.medium",
    "db.t4g.micro", "db.t4g.small",
    "db.t4g.medium",
  ]
}
