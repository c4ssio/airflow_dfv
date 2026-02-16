resource "aws_efs_file_system" "main" {
  creation_token = "${var.project_name}-efs"
  encrypted      = true

  performance_mode = "generalPurpose"
  throughput_mode  = "bursting"

  tags = { Name = "${var.project_name}-efs" }
}

resource "aws_efs_mount_target" "main" {
  count           = 2
  file_system_id  = aws_efs_file_system.main.id
  subnet_id       = aws_subnet.private[count.index].id
  security_groups = [aws_security_group.efs.id]
}

# Access point for Airflow data (/opt/airflow/data) — UID 50000 matches airflow user
resource "aws_efs_access_point" "data" {
  file_system_id = aws_efs_file_system.main.id

  posix_user {
    uid = 50000
    gid = 0
  }

  root_directory {
    path = "/airflow-data"
    creation_info {
      owner_uid   = 50000
      owner_gid   = 0
      permissions = "0755"
    }
  }

  tags = { Name = "${var.project_name}-efs-data" }
}

# Access point for Airflow logs (/opt/airflow/logs)
resource "aws_efs_access_point" "logs" {
  file_system_id = aws_efs_file_system.main.id

  posix_user {
    uid = 50000
    gid = 0
  }

  root_directory {
    path = "/airflow-logs"
    creation_info {
      owner_uid   = 50000
      owner_gid   = 0
      permissions = "0755"
    }
  }

  tags = { Name = "${var.project_name}-efs-logs" }
}
