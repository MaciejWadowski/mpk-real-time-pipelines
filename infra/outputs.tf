output "elastic_ip" {
  value = aws_eip.main.public_ip
}

output "ssh_command" {
  value = "ssh -i ~/.ssh/<your-key>.pem ubuntu@${aws_eip.main.public_ip}"
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

output "iam_passwords" {
  value     = { for u in local.users : u => random_password.users[u].result }
  sensitive = true
  description = "Retrieve with: terraform output -json iam_passwords"
}
