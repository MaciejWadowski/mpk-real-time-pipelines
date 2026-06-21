terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "tls_private_key" "ssh" {
  algorithm = "ED25519"
}

resource "aws_key_pair" "ssh" {
  key_name   = "mpk-key"
  public_key = tls_private_key.ssh.public_key_openssh
}

resource "local_sensitive_file" "private_key" {
  content         = tls_private_key.ssh.private_key_openssh
  filename        = "${path.module}/mpk-key.pem"
  file_permission = "0600"
}

data "aws_ami" "ubuntu_x86" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.ubuntu_x86.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.ssh.key_name
  vpc_security_group_ids = [aws_security_group.airflow.id]
  ebs_optimized          = true

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 10
    delete_on_termination = false
  }

  user_data = templatefile("${path.module}/startup.sh.tpl", {
    postgres_user        = var.postgres_user
    postgres_password    = var.postgres_password
    postgres_db          = var.postgres_db
    airflow_fernet_key   = var.airflow_fernet_key
    dbt_user             = var.dbt_user
    dbt_password         = var.dbt_password
    dbt_account          = var.dbt_account
    dbt_database         = var.dbt_database
    dbt_schema           = var.dbt_schema
    dbt_warehouse        = var.dbt_warehouse
    dbt_warehouse_stronk = var.dbt_warehouse_stronk
    dbt_role             = var.dbt_role
    tomtom_api_key       = var.tomtom_api_key
    repo_url             = var.repo_url
  })

  tags = {
    Name    = "mpk-airflow"
    Project = "mpk-real-time-pipelines"
  }
}

resource "aws_eip" "main" {
  instance = aws_instance.airflow.id
  domain   = "vpc"

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name = "mpk-airflow-eip"
  }
}
