locals {
  rds_host     = aws_db_instance.main.address
  rds_port     = "5432"
  rds_user     = var.db_username
  rds_password = random_password.db.result
  redis_host   = aws_elasticache_cluster.main.cache_nodes[0].address
  redis_port   = "6379"
  image        = "${aws_ecr_repository.main.repository_url}:latest"

  airflow_env = [
    { name = "AIRFLOW__CORE__EXECUTOR", value = "CeleryExecutor" },
    { name = "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION", value = "true" },
    { name = "AIRFLOW__CORE__LOAD_EXAMPLES", value = "false" },
    { name = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", value = "postgresql+psycopg2://${local.rds_user}:${local.rds_password}@${local.rds_host}:${local.rds_port}/airflow" },
    { name = "AIRFLOW__CELERY__BROKER_URL", value = "redis://${local.redis_host}:${local.redis_port}/0" },
    { name = "AIRFLOW__CELERY__RESULT_BACKEND", value = "db+postgresql://${local.rds_user}:${local.rds_password}@${local.rds_host}:${local.rds_port}/airflow" },
    { name = "AIRFLOW__CELERY__WORKER_CONCURRENCY", value = "2" },
    { name = "AIRFLOW__API__HOST", value = "0.0.0.0" },
    { name = "AIRFLOW__API__PORT", value = "8080" },
    { name = "AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX", value = "True" },
    { name = "AIRFLOW__LOGGING__BASE_LOG_FOLDER", value = "/opt/airflow/logs" },
    { name = "SEC_USER_AGENT", value = var.sec_user_agent },
    { name = "SEC_REQUESTS_PER_SECOND", value = "5" },
    { name = "SEC_TIMEOUT_SECONDS", value = "30" },
    { name = "SEC_LOCAL_DIR", value = "/opt/airflow/data/sec_raw" },
    { name = "POSTGRES_HOST", value = local.rds_host },
    { name = "POSTGRES_PORT", value = local.rds_port },
    { name = "POSTGRES_DB", value = "sec_data" },
    { name = "POSTGRES_USER", value = local.rds_user },
    { name = "POSTGRES_PASSWORD", value = local.rds_password },
    { name = "POSTGRES_SCHEMA", value = "sec_raw" },
    { name = "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS", value = "admin:admin" },
    { name = "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE", value = "/opt/airflow/simple_auth_manager_passwords.json.generated" },
    { name = "AIRFLOW_ADMIN_PASSWORD", value = random_password.airflow_admin.result },
  ]

  efs_volumes = [
    {
      name      = "data"
      fs_id     = aws_efs_file_system.main.id
      ap_id     = aws_efs_access_point.data.id
      container = "/opt/airflow/data"
    },
    {
      name      = "logs"
      fs_id     = aws_efs_file_system.main.id
      ap_id     = aws_efs_access_point.logs.id
      container = "/opt/airflow/logs"
    },
  ]
}

# --- ECS Cluster ---
resource "aws_ecs_cluster" "main" {
  name = "${var.project_name}-cluster"

  setting {
    name  = "containerInsights"
    value = "disabled"
  }

  tags = { Name = "${var.project_name}-ecs" }
}

# ============================================================
# DB init — one-shot task that creates the sec_data database
# and runs airflow db migrate
# ============================================================
resource "aws_ecs_task_definition" "init" {
  family                   = "${var.project_name}-init"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "init"
    image     = local.image
    essential = true

    entryPoint = ["/bin/bash", "-c"]
    command = [
      "pip install psycopg2-binary && python -c \"import psycopg2; conn = psycopg2.connect(host='${local.rds_host}', port=5432, user='${local.rds_user}', password='${local.rds_password}', dbname='airflow'); conn.autocommit = True; cur = conn.cursor(); cur.execute(\\\"SELECT 1 FROM pg_database WHERE datname='sec_data'\\\"); exists = cur.fetchone(); cur.execute(\\\"CREATE DATABASE sec_data\\\") if not exists else None; conn.close(); print('sec_data DB ready')\" && airflow db migrate"
    ]

    environment = local.airflow_env

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.airflow.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "init"
      }
    }
  }])
}

# ============================================================
# API Server
# ============================================================
resource "aws_ecs_task_definition" "api_server" {
  family                   = "${var.project_name}-api-server"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.api_server_cpu
  memory                   = var.api_server_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  dynamic "volume" {
    for_each = local.efs_volumes
    content {
      name = volume.value.name
      efs_volume_configuration {
        file_system_id          = volume.value.fs_id
        transit_encryption      = "ENABLED"
        authorization_config {
          access_point_id = volume.value.ap_id
          iam             = "ENABLED"
        }
      }
    }
  }

  container_definitions = jsonencode([{
    name      = "api-server"
    image     = local.image
    essential = true
    entryPoint = ["/bin/bash", "-c"]
    command   = ["echo \"{\\\"admin\\\": \\\"$AIRFLOW_ADMIN_PASSWORD\\\"}\" > /opt/airflow/simple_auth_manager_passwords.json.generated && exec airflow api-server"]

    portMappings = [{
      containerPort = 8080
      protocol      = "tcp"
    }]

    environment = local.airflow_env

    mountPoints = [for v in local.efs_volumes : {
      sourceVolume  = v.name
      containerPath = v.container
      readOnly      = false
    }]

    healthCheck = {
      command     = ["CMD-SHELL", "curl --fail http://localhost:8080/api/v2/monitor/health || exit 1"]
      interval    = 30
      timeout     = 10
      retries     = 5
      startPeriod = 60
    }

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.airflow.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "api-server"
      }
    }
  }])
}

resource "aws_ecs_service" "api_server" {
  name            = "${var.project_name}-api-server"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.api_server.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.ecs.id]
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api_server.arn
    container_name   = "api-server"
    container_port   = 8080
  }

  depends_on = [aws_lb_listener.http]
}

# ============================================================
# Scheduler
# ============================================================
resource "aws_ecs_task_definition" "scheduler" {
  family                   = "${var.project_name}-scheduler"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.scheduler_cpu
  memory                   = var.scheduler_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  dynamic "volume" {
    for_each = local.efs_volumes
    content {
      name = volume.value.name
      efs_volume_configuration {
        file_system_id          = volume.value.fs_id
        transit_encryption      = "ENABLED"
        authorization_config {
          access_point_id = volume.value.ap_id
          iam             = "ENABLED"
        }
      }
    }
  }

  container_definitions = jsonencode([{
    name      = "scheduler"
    image     = local.image
    essential = true
    command   = ["scheduler"]

    environment = local.airflow_env

    mountPoints = [for v in local.efs_volumes : {
      sourceVolume  = v.name
      containerPath = v.container
      readOnly      = false
    }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.airflow.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "scheduler"
      }
    }
  }])
}

resource "aws_ecs_service" "scheduler" {
  name            = "${var.project_name}-scheduler"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.scheduler.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.ecs.id]
  }
}

# ============================================================
# DAG Processor
# ============================================================
resource "aws_ecs_task_definition" "dag_processor" {
  family                   = "${var.project_name}-dag-processor"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.dag_processor_cpu
  memory                   = var.dag_processor_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  dynamic "volume" {
    for_each = local.efs_volumes
    content {
      name = volume.value.name
      efs_volume_configuration {
        file_system_id          = volume.value.fs_id
        transit_encryption      = "ENABLED"
        authorization_config {
          access_point_id = volume.value.ap_id
          iam             = "ENABLED"
        }
      }
    }
  }

  container_definitions = jsonencode([{
    name      = "dag-processor"
    image     = local.image
    essential = true
    command   = ["dag-processor"]

    environment = local.airflow_env

    mountPoints = [for v in local.efs_volumes : {
      sourceVolume  = v.name
      containerPath = v.container
      readOnly      = false
    }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.airflow.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "dag-processor"
      }
    }
  }])
}

resource "aws_ecs_service" "dag_processor" {
  name            = "${var.project_name}-dag-processor"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.dag_processor.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.ecs.id]
  }
}

# ============================================================
# Worker
# ============================================================
resource "aws_ecs_task_definition" "worker" {
  family                   = "${var.project_name}-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.worker_cpu
  memory                   = var.worker_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  dynamic "volume" {
    for_each = local.efs_volumes
    content {
      name = volume.value.name
      efs_volume_configuration {
        file_system_id          = volume.value.fs_id
        transit_encryption      = "ENABLED"
        authorization_config {
          access_point_id = volume.value.ap_id
          iam             = "ENABLED"
        }
      }
    }
  }

  container_definitions = jsonencode([{
    name      = "worker"
    image     = local.image
    essential = true
    command   = ["celery", "worker"]

    environment = local.airflow_env

    mountPoints = [for v in local.efs_volumes : {
      sourceVolume  = v.name
      containerPath = v.container
      readOnly      = false
    }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.airflow.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "worker"
      }
    }
  }])
}

resource "aws_ecs_service" "worker" {
  name            = "${var.project_name}-worker"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  enable_execute_command = true

  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.ecs.id]
  }
}

# ============================================================
# Triggerer
# ============================================================
resource "aws_ecs_task_definition" "triggerer" {
  family                   = "${var.project_name}-triggerer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.triggerer_cpu
  memory                   = var.triggerer_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  dynamic "volume" {
    for_each = local.efs_volumes
    content {
      name = volume.value.name
      efs_volume_configuration {
        file_system_id          = volume.value.fs_id
        transit_encryption      = "ENABLED"
        authorization_config {
          access_point_id = volume.value.ap_id
          iam             = "ENABLED"
        }
      }
    }
  }

  container_definitions = jsonencode([{
    name      = "triggerer"
    image     = local.image
    essential = true
    command   = ["triggerer"]

    environment = local.airflow_env

    mountPoints = [for v in local.efs_volumes : {
      sourceVolume  = v.name
      containerPath = v.container
      readOnly      = false
    }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.airflow.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "triggerer"
      }
    }
  }])
}

resource "aws_ecs_service" "triggerer" {
  name            = "${var.project_name}-triggerer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.triggerer.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.ecs.id]
  }
}
