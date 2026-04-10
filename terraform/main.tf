# ── IAM User ─────────────────────────────────────────────────────────────────

resource "aws_iam_user" "this" {
  name = var.iam_username
  path = "/"

  tags = {
    ManagedBy = "terraform"
  }
}

# ── AWS Managed policy attachments ───────────────────────────────────────────

# Broad read-only baseline across all AWS services
resource "aws_iam_user_policy_attachment" "read_only" {
  user       = aws_iam_user.this.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

# RDS custom policy attachment (see below for definition)
resource "aws_iam_user_policy_attachment" "rds_small_instances" {
  user       = aws_iam_user.this.name
  policy_arn = aws_iam_policy.rds_small_instances.arn
}

# Full S3 access (supersedes the read-only baseline for S3)
resource "aws_iam_user_policy_attachment" "s3_full" {
  user       = aws_iam_user.this.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# ── RDS custom policy: full access, small instance classes only ───────────────
#
# Strategy mirrors EC2:
#   1. Allow rds:* on all resources.
#   2. Deny the mutating actions that accept a DB instance class when the
#      requested class is NOT in the allowed (small) list.
#      Covered actions: CreateDBInstance, ModifyDBInstance,
#      RestoreDBInstanceFromDBSnapshot, RestoreDBInstanceToPointInTime,
#      CreateDBInstanceReadReplica.

data "aws_iam_policy_document" "rds_small_instances" {
  statement {
    sid       = "AllowAllRDSActions"
    effect    = "Allow"
    actions   = ["rds:*"]
    resources = ["*"]
  }

  statement {
    sid    = "DenyLargeRDSInstanceClass"
    effect = "Deny"
    actions = [
      "rds:CreateDBInstance",
      "rds:ModifyDBInstance",
      "rds:RestoreDBInstanceFromDBSnapshot",
      "rds:RestoreDBInstanceToPointInTime",
      "rds:CreateDBInstanceReadReplica",
    ]
    resources = ["*"]

    condition {
      test     = "StringNotLike"
      variable = "rds:DatabaseClass"
      values   = var.allowed_rds_instance_classes
    }
  }
}

resource "aws_iam_policy" "rds_small_instances" {
  name        = "${var.iam_username}-rds-small-instances"
  description = "Full RDS access; instance-class-changing actions are restricted to small classes only"
  policy      = data.aws_iam_policy_document.rds_small_instances.json
}

# ── EC2 custom policy: full access, small instance types only ─────────────────
#
# Strategy:
#   1. Allow ec2:* on all resources.
#   2. Explicitly Deny ec2:RunInstances on the instance resource type when the
#      requested instance type is NOT in the allowed (small) list.
#      An explicit Deny always wins over any Allow, so this effectively blocks
#      launching medium/large/xlarge instances regardless of other policies.

data "aws_iam_policy_document" "ec2_small_instances" {
  statement {
    sid       = "AllowAllEC2Actions"
    effect    = "Allow"
    actions   = ["ec2:*"]
    resources = ["*"]
  }

  statement {
    sid     = "DenyLargeInstanceLaunch"
    effect  = "Deny"
    actions = ["ec2:RunInstances"]
    # The instance type condition is only evaluated against the instance resource
    # ARN; supporting resources (AMI, subnet, SG …) are not affected.
    resources = ["arn:aws:ec2:*:*:instance/*"]

    condition {
      test     = "StringNotLike"
      variable = "ec2:InstanceType"
      values   = var.allowed_ec2_instance_types
    }
  }
}

resource "aws_iam_policy" "ec2_small_instances" {
  name        = "${var.iam_username}-ec2-small-instances"
  description = "Full EC2 access; RunInstances is restricted to small instance types only"
  policy      = data.aws_iam_policy_document.ec2_small_instances.json
}

resource "aws_iam_user_policy_attachment" "ec2_small_instances" {
  user       = aws_iam_user.this.name
  policy_arn = aws_iam_policy.ec2_small_instances.arn
}

# ── Optional programmatic access key ─────────────────────────────────────────

resource "aws_iam_access_key" "this" {
  count = var.create_access_key ? 1 : 0
  user  = aws_iam_user.this.name
}

resource "aws_iam_user_login_profile" "this" {
  user                    = aws_iam_user.this.name
  password_reset_required = false
}