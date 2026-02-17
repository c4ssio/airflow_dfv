#!/usr/bin/env bash
# Start the Airflow stack after a stop. Reverses stop_stack.sh.
#
# Usage: ./scripts/aws/start_stack.sh [--create-redis]
set -euo pipefail

REGION="us-east-1"
CLUSTER="sec-scraper-cluster"
PROJECT="sec-scraper"
CREATE_REDIS=false

for arg in "$@"; do
  case $arg in
    --create-redis) CREATE_REDIS=true ;;
  esac
done

# 1. Start RDS
echo "==> Starting RDS instance..."
RDS_ID=$(aws rds describe-db-instances --region "$REGION" \
  --query "DBInstances[?DBInstanceIdentifier=='${PROJECT}-db'].DBInstanceIdentifier" \
  --output text 2>/dev/null)
if [ -n "$RDS_ID" ] && [ "$RDS_ID" != "None" ]; then
  RDS_STATUS=$(aws rds describe-db-instances --region "$REGION" \
    --db-instance-identifier "$RDS_ID" \
    --query 'DBInstances[0].DBInstanceStatus' --output text)
  if [ "$RDS_STATUS" = "stopped" ]; then
    aws rds start-db-instance --db-instance-identifier "$RDS_ID" --region "$REGION" --no-cli-pager > /dev/null
    echo "    RDS ${RDS_ID} starting... (takes 3-5 minutes)"
  else
    echo "    RDS ${RDS_ID} is ${RDS_STATUS}, skipping."
  fi
else
  echo "    No RDS instance found. Run terraform apply to recreate."
fi

# 2. Recreate ElastiCache if it was deleted
if [ "$CREATE_REDIS" = true ]; then
  echo "==> Recreating ElastiCache cluster..."
  # Get subnet group and SG from existing resources
  VPC_ID=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=${PROJECT}-vpc" \
    --query 'Vpcs[0].VpcId' --output text --region "$REGION")
  REDIS_SG=$(aws ec2 describe-security-groups --region "$REGION" \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=tag:Name,Values=${PROJECT}-redis-sg" \
    --query 'SecurityGroups[0].GroupId' --output text)

  aws elasticache create-cache-cluster \
    --cache-cluster-id "${PROJECT}-redis" \
    --engine redis \
    --engine-version "7.1" \
    --cache-node-type cache.t4g.micro \
    --num-cache-nodes 1 \
    --cache-subnet-group-name "${PROJECT}-redis-subnet" \
    --security-group-ids "$REDIS_SG" \
    --region "$REGION" \
    --no-cli-pager > /dev/null 2>&1 || echo "    Redis cluster already exists."
  echo "    Redis cluster creating... (takes 3-5 minutes)"
else
  # Check if Redis exists
  REDIS_STATUS=$(aws elasticache describe-cache-clusters --region "$REGION" \
    --cache-cluster-id "${PROJECT}-redis" \
    --query 'CacheClusters[0].CacheClusterStatus' --output text 2>/dev/null || echo "not-found")
  if [ "$REDIS_STATUS" = "not-found" ]; then
    echo "==> WARNING: ElastiCache cluster not found. Run with --create-redis to recreate."
  else
    echo "==> ElastiCache is ${REDIS_STATUS}."
  fi
fi

# 3. Wait for RDS to be available before starting ECS
echo "==> Waiting for RDS to become available..."
RDS_STATUS="starting"
TRIES=0
MAX_TRIES=60  # 5 minutes
while [ "$RDS_STATUS" != "available" ] && [ $TRIES -lt $MAX_TRIES ]; do
  sleep 5
  TRIES=$((TRIES + 1))
  RDS_STATUS=$(aws rds describe-db-instances --region "$REGION" \
    --db-instance-identifier "$RDS_ID" \
    --query 'DBInstances[0].DBInstanceStatus' --output text 2>/dev/null || echo "unknown")
  if [ $((TRIES % 12)) -eq 0 ]; then
    echo "    Still waiting... RDS status: ${RDS_STATUS} (${TRIES}s)"
  fi
done

if [ "$RDS_STATUS" = "available" ]; then
  echo "    RDS is available."
else
  echo "    WARNING: RDS status is ${RDS_STATUS} after ${TRIES} checks. ECS services may fail to connect."
  echo "    Continuing anyway — services will retry."
fi

# 4. Scale ECS services back to 1
echo "==> Scaling ECS services to 1..."
for svc in api-server scheduler dag-processor worker triggerer; do
  aws ecs update-service \
    --cluster "$CLUSTER" \
    --service "${PROJECT}-${svc}" \
    --desired-count 1 \
    --region "$REGION" \
    --no-cli-pager > /dev/null 2>&1 || true
  echo "    ${PROJECT}-${svc} → 1"
done

echo ""
echo "==> Stack starting up."
echo "    Services will take 2-3 minutes to stabilize."
echo "    Check health: curl http://sec-scraper-alb-*.us-east-1.elb.amazonaws.com:8080/api/v2/monitor/health"
