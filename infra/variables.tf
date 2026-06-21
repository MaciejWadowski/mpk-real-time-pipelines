variable "region" {
  default = "eu-central-1"
}

variable "instance_type" {
  default = "c7i-flex.large"
}

variable "my_ip" {
  description = "Your home IP in CIDR notation, e.g. 1.2.3.4/32"
}

variable "repo_url" {
  default = "https://github.com/MaciejWadowski/mpk-real-time-pipelines.git"
}

variable "postgres_user" {
  default = "airflow"
}

variable "postgres_password" {
  sensitive = true
}

variable "postgres_db" {
  default = "airflow"
}

variable "airflow_fernet_key" {
  sensitive   = true
  description = "Generate with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
}

variable "dbt_user" {}

variable "dbt_password" {
  sensitive = true
}

variable "dbt_account" {
  description = "Snowflake account identifier"
}

variable "dbt_database" {
  default = "GTFS_TEST"
}

variable "dbt_schema" {
  default = "SCHEDULE_DATA_MARTS"
}

variable "dbt_warehouse" {
  default = "COMPUTE_WH"
}

variable "dbt_warehouse_stronk" {
  default = "COMPUTE_WH"
}

variable "dbt_role" {
  default = "GTFS_UPLOADER_ROLE"
}

variable "tomtom_api_key" {
  sensitive   = true
  description = "JSON array of TomTom API keys, e.g. [\"key1\",\"key2\"]"
}

variable "iam_initial_password" {
  sensitive   = true
  description = "Initial console password for all IAM users (password_reset_required=true)"
}

variable "iam_users_csv" {
  description = "Comma-separated list of IAM usernames, e.g. USER1,USER2,USER3"
}
