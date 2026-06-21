terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_ami" "ubuntu_arm" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-arm64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["arm64"]
  }
}

resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.ubuntu_arm.id
  instance_type          = var.instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.airflow.id]
  ebs_optimized          = true

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 50
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

  tags = {
    Name = "mpk-airflow-eip"
  }
}
