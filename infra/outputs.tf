output "elastic_ip" {
  value = aws_eip.main.public_ip
}

output "ssh_command" {
  value = "ssh -i ${local_sensitive_file.private_key.filename} ubuntu@${aws_eip.main.public_ip}"
}

output "airflow_ui" {
  value = "http://${aws_eip.main.public_ip}:8080"
}

output "flower_ui" {
  value = "http://${aws_eip.main.public_ip}:5555"
}

output "startup_log" {
  value = "sudo journalctl -u cloud-final -f"
  description = "Run this on the EC2 to watch the startup script progress"
}

output "iam_initial_password" {
  value       = var.iam_initial_password
  sensitive   = true
  description = "Shared initial password for all IAM users. Retrieve with: terraform output iam_initial_password"
}
