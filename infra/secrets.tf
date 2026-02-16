resource "aws_secretsmanager_secret" "app" {
  name                    = "${var.project_name}/app-config"
  recovery_window_in_days = 0 # Immediate delete for ephemeral stack

  tags = { Name = "${var.project_name}-secrets" }
}

resource "aws_secretsmanager_secret_version" "app" {
  secret_id = aws_secretsmanager_secret.app.id
  secret_string = jsonencode({
    POSTGRES_HOST     = aws_db_instance.main.address
    POSTGRES_PORT     = "5432"
    POSTGRES_DB       = "sec_data"
    POSTGRES_USER     = var.db_username
    POSTGRES_PASSWORD = random_password.db.result
    POSTGRES_SCHEMA   = "sec_raw"
    SEC_USER_AGENT                     = var.sec_user_agent
    AIRFLOW_ADMIN_PASSWORD             = random_password.airflow_admin.result
    AIRFLOW__CORE__FERNET_KEY          = base64encode(random_password.fernet_key.result)
    AIRFLOW__CORE__INTERNAL_API_SECRET_KEY = random_password.internal_api_secret.result
    AIRFLOW__API_AUTH__JWT_SECRET       = random_password.internal_api_secret.result
  })
}
