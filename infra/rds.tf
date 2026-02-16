resource "random_password" "db" {
  length  = 24
  special = false
}

resource "random_password" "airflow_admin" {
  length  = 24
  special = false
}

resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-db-subnet"
  subnet_ids = aws_subnet.private[*].id

  tags = { Name = "${var.project_name}-db-subnet-group" }
}

resource "aws_db_instance" "main" {
  identifier = "${var.project_name}-db"

  engine         = "postgres"
  engine_version = "15"
  instance_class = var.rds_instance_class

  allocated_storage = var.rds_allocated_storage
  storage_type      = "gp3"

  db_name  = "airflow"
  username = var.db_username
  password = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  multi_az            = false
  publicly_accessible = false

  skip_final_snapshot       = true
  final_snapshot_identifier = null
  deletion_protection       = false

  backup_retention_period = 0

  tags = { Name = "${var.project_name}-rds" }
}
