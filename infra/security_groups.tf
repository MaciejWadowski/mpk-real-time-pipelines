resource "aws_security_group" "airflow" {
  name        = "mpk-airflow"
  description = "SSH and Airflow UI access from developer machine"

  ingress {
    description = "All traffic (temporary)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    #TODO restrict to our ip's in future
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    #TODO restrict to our ip's in future
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "mpk-airflow"
    Project = "mpk-real-time-pipelines"
  }
}
