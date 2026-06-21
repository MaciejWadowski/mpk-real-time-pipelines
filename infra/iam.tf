locals {
  users = ["MWADOWSKI", "FSZOLDRA", "POSTROWSKI", "KNI", "FKONKEL"]
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
      {
        Sid    = "EC2LowCostOnly"
        Effect = "Allow"
        Action = [
          "ec2:DescribeInstances",
          "ec2:DescribeImages",
          "ec2:DescribeKeyPairs",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSubnets",
          "ec2:DescribeVpcs",
          "ec2:DescribeAvailabilityZones",
          "ec2:DescribeInstanceTypes",
          "ec2:DescribeAddresses",
          "ec2:RunInstances",
          "ec2:StopInstances",
          "ec2:StartInstances",
          "ec2:TerminateInstances",
          "ec2:RebootInstances",
          "ec2:CreateTags",
          "ec2:AllocateAddress",
          "ec2:AssociateAddress",
          "ec2:ReleaseAddress",
          "ec2:CreateSecurityGroup",
          "ec2:AuthorizeSecurityGroupIngress",
          "ec2:AuthorizeSecurityGroupEgress",
          "ec2:DeleteSecurityGroup",
          "ec2:CreateKeyPair",
          "ec2:DeleteKeyPair",
          "ec2:ImportKeyPair",
        ]
        Resource = "*"
        Condition = {
          StringLike = {
            "ec2:InstanceType" = ["t2.*", "t3.*", "t4g.*"]
          }
        }
      },
      {
        Sid    = "EC2DescribeNoCondition"
        Effect = "Allow"
        Action = [
          "ec2:DescribeInstances",
          "ec2:DescribeImages",
          "ec2:DescribeKeyPairs",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSubnets",
          "ec2:DescribeVpcs",
          "ec2:DescribeAvailabilityZones",
          "ec2:DescribeInstanceTypes",
          "ec2:DescribeAddresses",
          "ec2:DescribeVolumes",
          "ec2:DescribeSnapshots",
        ]
        Resource = "*"
      },
      {
        Sid    = "RDSLowCostOnly"
        Effect = "Allow"
        Action = [
          "rds:CreateDBInstance",
          "rds:DeleteDBInstance",
          "rds:DescribeDBInstances",
          "rds:DescribeDBClusters",
          "rds:ModifyDBInstance",
          "rds:StopDBInstance",
          "rds:StartDBInstance",
          "rds:CreateDBSubnetGroup",
          "rds:DescribeDBSubnetGroups",
          "rds:ListTagsForResource",
          "rds:AddTagsToResource",
        ]
        Resource = "*"
        Condition = {
          StringLike = {
            "rds:DatabaseClass" = ["db.t3.*", "db.t4g.*"]
          }
        }
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

resource "random_password" "users" {
  for_each = toset(local.users)

  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>?"
  min_upper        = 2
  min_lower        = 2
  min_numeric      = 2
  min_special      = 2
}

resource "aws_iam_user" "developers" {
  for_each = toset(local.users)

  name = each.key
  tags = { Project = "mpk-real-time-pipelines" }
}

resource "aws_iam_user_login_profile" "developers" {
  for_each = toset(local.users)

  user                    = aws_iam_user.developers[each.key].name
  password                = random_password.users[each.key].result
  password_reset_required = true
}

resource "aws_iam_user_group_membership" "developers" {
  for_each = toset(local.users)

  user   = aws_iam_user.developers[each.key].name
  groups = [aws_iam_group.developers.name]
}
