#!/usr/bin/env bash
# Full deploy: terraform apply → build/push image → run init → force new deployments.
# Usage: ./scripts/aws/deploy.sh [--skip-build] [--skip-init]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INFRA_DIR="$REPO_ROOT/infra"

REGION="us-east-1"
PROJECT="sec-scraper"
SKIP_BUILD=false
SKIP_INIT=false
SNAPSHOT=""
for arg in "$@"; do
  case $arg in
    --skip-build)  SKIP_BUILD=true ;;
    --skip-init)   SKIP_INIT=true ;;
    --snapshot=*)  SNAPSHOT="${arg#*=}" ;;
  esac
done

# Auto-detect latest RDS snapshot if not specified
if [ -z "$SNAPSHOT" ]; then
  echo "==> Checking for latest RDS snapshot..."
  SNAPSHOT=$(aws rds describe-db-snapshots --region "$REGION" \
    --query "reverse(sort_by(DBSnapshots[?starts_with(DBSnapshotIdentifier, '${PROJECT}-db-pre-teardown-')], &SnapshotCreateTime))[0].DBSnapshotIdentifier" \
    --output text 2>/dev/null || echo "None")
  if [ "$SNAPSHOT" = "None" ] || [ -z "$SNAPSHOT" ]; then
    echo "    No pre-teardown snapshot found. Starting fresh."
    SNAPSHOT=""
  else
    echo "    Found latest snapshot: ${SNAPSHOT}"
  fi
fi

echo "==> Terraform init + apply..."
cd "$INFRA_DIR"
terraform init -input=false
if [ -n "$SNAPSHOT" ]; then
  terraform apply -auto-approve -var "rds_snapshot_identifier=${SNAPSHOT}"
else
  terraform apply -auto-approve
fi

# Get outputs
ECR_URL=$(terraform output -raw ecr_repository_url)
CLUSTER=$(terraform output -raw ecs_cluster_name)
REGION=$(terraform output -raw 2>/dev/null || echo "us-east-1")

if [ "$SKIP_BUILD" = false ]; then
  echo "==> Building and pushing Docker image..."
  "$SCRIPT_DIR/build_and_push.sh"
fi

if [ "$SKIP_INIT" = false ]; then
  echo "==> Running init task (db migrate + create sec_data)..."
  INIT_TASK_DEF=$(aws ecs list-task-definitions --family-prefix sec-scraper-init --sort DESC --query 'taskDefinitionArns[0]' --output text --region us-east-1)

  # Get private subnets and ECS security group from terraform state
  SUBNETS=$(terraform output -json 2>/dev/null | python3 -c "
import json, sys
# Read subnet IDs directly from state
" 2>/dev/null || true)

  # Fallback: get from AWS directly
  VPC_ID=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=sec-scraper-vpc" --query 'Vpcs[0].VpcId' --output text --region us-east-1)
  SUBNETS=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" "Name=tag:Name,Values=*private*" --query 'Subnets[*].SubnetId' --output text --region us-east-1 | tr '\t' ',')
  SG=$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$VPC_ID" "Name=tag:Name,Values=sec-scraper-ecs-sg" --query 'SecurityGroups[0].GroupId' --output text --region us-east-1)

  aws ecs run-task \
    --cluster "$CLUSTER" \
    --task-definition "$INIT_TASK_DEF" \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG]}" \
    --region us-east-1

  echo "    Init task started. Check CloudWatch /ecs/sec-scraper for logs."
  echo "    Waiting 60s for init to complete..."
  sleep 60
fi

echo "==> Forcing new deployments on all services..."
for svc in api-server scheduler dag-processor worker triggerer; do
  aws ecs update-service \
    --cluster "$CLUSTER" \
    --service "sec-scraper-${svc}" \
    --force-new-deployment \
    --region us-east-1 \
    --no-cli-pager > /dev/null
  echo "    Redeployed: sec-scraper-${svc}"
done

ALB_URL=$(terraform output -raw alb_url)
echo ""
echo "==> Deploy complete!"
echo "    Airflow UI: $ALB_URL"
echo "    (Services may take 2-3 minutes to stabilize)"
