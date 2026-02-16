#!/usr/bin/env bash
# Build the Airflow Docker image and push to ECR.
# Usage: ./scripts/aws/build_and_push.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INFRA_DIR="$REPO_ROOT/infra"

# Get ECR repo URL from terraform output
ECR_URL=$(cd "$INFRA_DIR" && terraform output -raw ecr_repository_url)
REGION=$(cd "$INFRA_DIR" && terraform output -raw 2>/dev/null | grep -q aws_region && terraform output -raw aws_region || echo "us-east-1")
ACCOUNT_ID=$(echo "$ECR_URL" | cut -d. -f1)

echo "==> Logging into ECR..."
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ECR_URL"

echo "==> Building image..."
docker build -t sec-scraper-airflow:latest "$REPO_ROOT"

echo "==> Tagging..."
docker tag sec-scraper-airflow:latest "${ECR_URL}:latest"

echo "==> Pushing..."
docker push "${ECR_URL}:latest"

echo "==> Done. Image pushed to ${ECR_URL}:latest"
