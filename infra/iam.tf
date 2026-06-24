locals {
  users = split(",", var.iam_users_csv)
}

# ── Group ──────────────────────────────────────────────────────────────────────

resource "aws_iam_group" "developers" {
  name = "mpk-developers"
}

resource "aws_iam_group_policy_attachment" "developer_policy" {
  group      = aws_iam_group.developers.name
  policy_arn = aws_iam_policy.developer.arn
}

# ── Policy ─────────────────────────────────────────────────────────────────────

resource "aws_iam_policy" "developer" {
  name        = "mpk-developer-policy"
  description = "Scoped access for MPK project developers — low-cost EC2/RDS/S3/CloudWatch"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Instance launch — condition only applies to RunInstances/Start/Stop/Terminate
      {
        Sid    = "EC2FreeTierInstances"
        Effect = "Allow"
        Action = [
          "ec2:RunInstances",
          "ec2:StopInstances",
          "ec2:StartInstances",
          "ec2:TerminateInstances",
          "ec2:RebootInstances",
        ]
        Resource = "*"
        Condition = {
          StringLike = {
            "ec2:InstanceType" = [
              "t2.micro",
              "t3.micro", "t3.small",
              "t4g.micro", "t4g.small",
              "m7i-flex.large",
              "c7i-flex.large",
            ]
          }
        }
      },
      # Describe — no condition needed (condition keys not applicable)
      {
        Sid    = "EC2Describe"
        Effect = "Allow"
        Action = [
          "ec2:Describe*",
        ]
        Resource = "*"
      },
      # Networking — security groups, VPC, subnets, routing, IGW, EIP, key pairs
      {
        Sid    = "EC2Networking"
        Effect = "Allow"
        Action = [
          "ec2:CreateTags",
          "ec2:DeleteTags",
          # Security groups
          "ec2:CreateSecurityGroup",
          "ec2:DeleteSecurityGroup",
          "ec2:AuthorizeSecurityGroupIngress",
          "ec2:AuthorizeSecurityGroupEgress",
          "ec2:RevokeSecurityGroupIngress",
          "ec2:RevokeSecurityGroupEgress",
          "ec2:UpdateSecurityGroupRuleDescriptionsIngress",
          "ec2:UpdateSecurityGroupRuleDescriptionsEgress",
          # VPC
          "ec2:CreateVpc",
          "ec2:DeleteVpc",
          "ec2:ModifyVpcAttribute",
          # Subnets
          "ec2:CreateSubnet",
          "ec2:DeleteSubnet",
          "ec2:ModifySubnetAttribute",
          # Route tables
          "ec2:CreateRouteTable",
          "ec2:DeleteRouteTable",
          "ec2:AssociateRouteTable",
          "ec2:DisassociateRouteTable",
          "ec2:CreateRoute",
          "ec2:DeleteRoute",
          # Internet gateway
          "ec2:CreateInternetGateway",
          "ec2:DeleteInternetGateway",
          "ec2:AttachInternetGateway",
          "ec2:DetachInternetGateway",
          # Elastic IPs
          "ec2:AllocateAddress",
          "ec2:AssociateAddress",
          "ec2:DisassociateAddress",
          "ec2:ReleaseAddress",
          # Key pairs
          "ec2:CreateKeyPair",
          "ec2:DeleteKeyPair",
          "ec2:ImportKeyPair",
          # Network interfaces
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:AttachNetworkInterface",
          "ec2:DetachNetworkInterface",
          "ec2:ModifyNetworkInterfaceAttribute",
          # Volumes
          "ec2:CreateVolume",
          "ec2:DeleteVolume",
          "ec2:AttachVolume",
          "ec2:DetachVolume",
          "ec2:ModifyVolume",
        ]
        Resource = "*"
      },
      # RDS create — condition on instance class only applies to CreateDBInstance
      {
        Sid      = "RDSCreate"
        Effect   = "Allow"
        Action   = ["rds:CreateDBInstance"]
        Resource = "*"
        Condition = {
          StringLike = {
            "rds:DatabaseClass" = ["db.t2.*", "db.t3.*", "db.t4g.*"]
          }
        }
      },
      # RDS manage/read — no instance class condition (operates on existing resources)
      {
        Sid    = "RDSManage"
        Effect = "Allow"
        Action = [
          "rds:DeleteDBInstance",
          "rds:DescribeDBInstances",
          "rds:DescribeDBClusters",
          "rds:ModifyDBInstance",
          "rds:StopDBInstance",
          "rds:StartDBInstance",
          "rds:CreateDBSubnetGroup",
          "rds:DescribeDBSubnetGroups",
          "rds:DeleteDBSubnetGroup",
          "rds:ListTagsForResource",
          "rds:AddTagsToResource",
          "rds:DescribeDBEngineVersions",
          "rds:DescribeOrderableDBInstanceOptions",
        ]
        Resource = "*"
      },
      {
        Sid      = "S3Full"
        Effect   = "Allow"
        Action   = ["s3:*"]
        Resource = "*"
      },
      {
        Sid    = "CloudWatchBasic"
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "cloudwatch:GetMetricData",
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:ListMetrics",
          "cloudwatch:PutDashboard",
          "cloudwatch:GetDashboard",
          "cloudwatch:ListDashboards",
          "cloudwatch:DescribeAlarms",
          "cloudwatch:PutMetricAlarm",
          "cloudwatch:DeleteAlarms",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams",
          "logs:GetLogEvents",
          "logs:FilterLogEvents",
          "logs:DeleteLogGroup",
          "logs:DeleteLogStream",
        ]
        Resource = "*"
      },
      {
        Sid    = "IAMSelfService"
        Effect = "Allow"
        Action = [
          "iam:GetUser",
          "iam:ChangePassword",
          "iam:CreateAccessKey",
          "iam:DeleteAccessKey",
          "iam:ListAccessKeys",
          "iam:UpdateAccessKey",
          "iam:GetAccountPasswordPolicy",
        ]
        Resource = "arn:aws:iam::*:user/$${aws:username}"
      },
    ]
  })
}

# ── Users ──────────────────────────────────────────────────────────────────────

resource "aws_iam_user" "developers" {
  for_each = toset(local.users)

  name = each.key
  tags = { Project = "mpk-real-time-pipelines" }
}

resource "null_resource" "user_passwords" {
  for_each = toset(local.users)

  triggers = {
    user     = aws_iam_user.developers[each.key].name
    password = var.iam_initial_password
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      output=$(aws iam create-login-profile \
        --user-name ${each.key} \
        --password "${var.iam_initial_password}" \
        --password-reset-required 2>&1) && echo "$output" || {
        if echo "$output" | grep -q EntityAlreadyExists; then
          aws iam update-login-profile \
            --user-name ${each.key} \
            --password "${var.iam_initial_password}" \
            --password-reset-required
        else
          echo "$output" >&2
          exit 1
        fi
      }
    EOT
  }

  depends_on = [aws_iam_user.developers]
}

resource "aws_iam_user_group_membership" "developers" {
  for_each = toset(local.users)

  user   = aws_iam_user.developers[each.key].name
  groups = [aws_iam_group.developers.name]
}
