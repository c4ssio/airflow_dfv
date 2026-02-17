output "alb_url" {
  description = "Airflow UI URL"
  value       = "http://${aws_lb.main.dns_name}:8080"
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = aws_db_instance.main.endpoint
}

output "redis_endpoint" {
  description = "ElastiCache Redis endpoint"
  value       = "${aws_elasticache_cluster.main.cache_nodes[0].address}:${aws_elasticache_cluster.main.cache_nodes[0].port}"
}

output "ecr_repository_url" {
  description = "ECR repository URL"
  value       = aws_ecr_repository.main.repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.main.name
}

output "db_password" {
  description = "Generated DB password"
  value       = random_password.db.result
  sensitive   = true
}

output "airflow_admin_password" {
  description = "Generated Airflow admin UI password (username: admin)"
  value       = random_password.airflow_admin.result
  sensitive   = true
}

## Jumpbox outputs temporarily removed for rebuild
## output "jumpbox_public_ip" {
##   description = "Jumpbox Elastic IP (persists across stop/start)"
##   value       = aws_eip.jumpbox.public_ip
## }
##
## output "jumpbox_instance_id" {
##   description = "Jumpbox EC2 instance ID (for SSM Session Manager)"
##   value       = aws_instance.jumpbox.id
## }
