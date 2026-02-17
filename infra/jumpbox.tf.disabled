# --- Jumpbox EC2 Instance ---
# Small instance in public subnet for running terraform, ECS exec, and ad-hoc admin.
# Uses an Elastic IP so the address persists across stop/start cycles.

data "aws_ssm_parameter" "al2023_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_security_group" "jumpbox" {
  name_prefix = "${var.project_name}-jumpbox-"
  vpc_id      = aws_vpc.main.id
  description = "Jumpbox - SSH inbound from allowed CIDR"

  ingress {
    description = "SSH from allowed CIDR"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-jumpbox-sg" }

  lifecycle { create_before_destroy = true }
}

resource "aws_iam_role" "jumpbox" {
  name = "${var.project_name}-jumpbox-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "${var.project_name}-jumpbox-role" }
}

resource "aws_iam_role_policy_attachment" "jumpbox_ssm" {
  role       = aws_iam_role.jumpbox.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "jumpbox_admin" {
  role       = aws_iam_role.jumpbox.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

resource "aws_iam_instance_profile" "jumpbox" {
  name = "${var.project_name}-jumpbox"
  role = aws_iam_role.jumpbox.name
}

resource "aws_instance" "jumpbox" {
  ami                         = data.aws_ssm_parameter.al2023_ami.value
  instance_type               = "t3.micro"
  key_name                    = var.jumpbox_key_name
  subnet_id                   = aws_subnet.public[0].id
  vpc_security_group_ids      = [aws_security_group.jumpbox.id]
  iam_instance_profile        = aws_iam_instance_profile.jumpbox.name
  associate_public_ip_address = false # Using EIP instead

  user_data = <<-EOF
    #!/bin/bash
    set -ex
    yum install -y yum-utils git docker
    yum-config-manager --add-repo https://rpm.releases.hashicorp.com/AmazonLinux/hashicorp.repo
    yum install -y terraform
    curl -o /tmp/session-manager-plugin.rpm \
      "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/linux_64bit/session-manager-plugin.rpm"
    yum install -y /tmp/session-manager-plugin.rpm
    systemctl enable docker && systemctl start docker
    usermod -aG docker ec2-user
    su - ec2-user -c "mkdir -p ~/projects"
    echo "Jumpbox setup complete" > /tmp/setup-complete
  EOF

  tags = { Name = "${var.project_name}-jumpbox", Environment = "ephemeral" }
}

resource "aws_eip" "jumpbox" {
  domain = "vpc"
  tags   = { Name = "${var.project_name}-jumpbox-eip", Environment = "ephemeral" }
}

resource "aws_eip_association" "jumpbox" {
  instance_id   = aws_instance.jumpbox.id
  allocation_id = aws_eip.jumpbox.id
}
