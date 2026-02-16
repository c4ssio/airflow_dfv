variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used as prefix for resources"
  type        = string
  default     = "sec-scraper"
}

variable "allowed_cidr" {
  description = "CIDR block allowed to access the ALB (your IP/32)"
  type        = string
  default     = "0.0.0.0/0"
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "rds_instance_class" {
  description = "RDS instance type"
  type        = string
  default     = "db.t4g.micro"
}

variable "rds_allocated_storage" {
  description = "RDS storage in GB"
  type        = number
  default     = 20
}

variable "redis_node_type" {
  description = "ElastiCache node type"
  type        = string
  default     = "cache.t4g.micro"
}

variable "db_username" {
  description = "Master DB username"
  type        = string
  default     = "airflow"
}

variable "sec_user_agent" {
  description = "SEC API user agent string"
  type        = string
  default     = "drclive SEC scraper (you@drclive.net)"
}

# ECS task sizing
variable "api_server_cpu" {
  type    = number
  default = 512
}
variable "api_server_memory" {
  type    = number
  default = 1024
}
variable "scheduler_cpu" {
  type    = number
  default = 256
}
variable "scheduler_memory" {
  type    = number
  default = 512
}
variable "dag_processor_cpu" {
  type    = number
  default = 256
}
variable "dag_processor_memory" {
  type    = number
  default = 512
}
variable "worker_cpu" {
  type    = number
  default = 1024
}
variable "worker_memory" {
  type    = number
  default = 2048
}
variable "triggerer_cpu" {
  type    = number
  default = 256
}
variable "triggerer_memory" {
  type    = number
  default = 512
}

variable "jumpbox_key_name" {
  description = "EC2 key pair name for the jumpbox"
  type        = string
  default     = "remote_cursor_key"
}
