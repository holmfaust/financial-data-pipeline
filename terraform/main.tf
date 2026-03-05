terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "FinancialDataPipeline"
      Environment = "Demo"
      ManagedBy   = "Terraform"
    }
  }
}

variable "aws_region" {
  default = "ap-southeast-1"
}

variable "instance_type" {
  # t3.xlarge is 4 vCPUs, 16GB RAM (about $0.16/hr)
  # Enough to comfortably run Spark, Kafka, Postgres, Redis, App in Docker
  default = "t3.xlarge"
}

# ------------------------------------------------------------------------------
# Networking (Default VPC is fine for demo)
# ------------------------------------------------------------------------------
data "aws_vpc" "default" {
  default = true
}

resource "aws_security_group" "demo_sg" {
  name        = "fin-pipeline-demo-sg"
  description = "Allow SSH and Dashboard access"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH Access"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # For demo only. In prod, lock this to your IP.
  }

  ingress {
    description = "Streamlit Dashboard"
    from_port   = 8501
    to_port     = 8501
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ------------------------------------------------------------------------------
# EC2 Instance
# ------------------------------------------------------------------------------
# Find latest Ubuntu 22.04 AMI
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

# Create an SSH key pair
resource "tls_private_key" "demo_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "demo_keypair" {
  key_name   = "fin-pipeline-demo-key"
  public_key = tls_private_key.demo_key.public_key_openssh
}

resource "aws_instance" "app_server" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = var.instance_type
  key_name      = aws_key_pair.demo_keypair.key_name

  vpc_security_group_ids = [aws_security_group.demo_sg.id]
  associate_public_ip_address = true
  
  root_block_device {
    volume_size = 50 # 50 GB should be plenty
    volume_type = "gp3"
  }

  # Script to install Docker and Git on startup
  user_data = <<-EOF
              #!/bin/bash
              apt-get update -y
              
              # Install Docker
              apt-get install -y apt-transport-https ca-certificates curl software-properties-common git
              curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
              add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
              apt-get update -y
              apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

              # Install old docker-compose 
              curl -L "https://github.com/docker/compose/releases/download/v2.24.5/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
              chmod +x /usr/local/bin/docker-compose

              systemctl enable docker
              systemctl start docker
              usermod -aG docker ubuntu
              EOF

  tags = {
    Name = "Financial Pipeline - Demo EC2"
  }
}

# ------------------------------------------------------------------------------
# Outputs Configuration (Show IP and Key)
# ------------------------------------------------------------------------------
output "public_ip" {
  description = "The public IP of the EC2 instance"
  value       = aws_instance.app_server.public_ip
}

output "dashboard_url" {
  description = "The URL for the Streamlit Dashboard"
  value       = "http://${aws_instance.app_server.public_ip}:8501"
}

output "ssh_command" {
  description = "Command to SSH into the instance"
  value       = "ssh -i fin-pipeline-demo.pem ubuntu@${aws_instance.app_server.public_ip}"
}

# Save private key to local file
resource "local_file" "private_key" {
  content         = tls_private_key.demo_key.private_key_pem
  filename        = "${path.module}/fin-pipeline-demo.pem"
  file_permission = "0400"
}
