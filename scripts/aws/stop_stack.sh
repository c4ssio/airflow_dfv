#!/usr/bin/env bash
# Stop the Airflow stack to save costs. Preserves all data and config.
#
# What gets stopped:
#   - ECS services scaled to 0     (Fargate: $0 when no tasks running)
#   - RDS instance stopped          (free when stopped, auto-restarts after 7 days)
#   - Jumpbox EC2 stopped           (free when stopped, EIP retained)
#
# What keeps running (minimal cost):
#   - ALB (~$16/mo)                 — kept so DNS stays stable
#   - ElastiCache Redis (~$12/mo)   — cannot be stopped, only deleted
#   - NAT Gateway (~$32/mo)         — needed for private subnets
#   - EFS                           — pennies unless storing data
#   - EIP                           — free while jumpbox is stopped (attached to stopped instance)
#
# To also stop ElastiCache (saves ~$12/mo but loses broker state):
#   ./scripts/aws/stop_stack.sh --delete-redis
#
# Usage: ./scripts/aws/stop_stack.sh [--delete-redis] [--yes]
set -euo pipefail

REGION="us-east-1"
CLUSTER="sec-scraper-cluster"
PROJECT="sec-scraper"
DELETE_REDIS=false
AUTO_YES=false

for arg in "$@"; do
  case $arg in
    --delete-redis) DELETE_REDIS=true ;;
    --yes)          AUTO_YES=true ;;
  esac
done

if [ "$AUTO_YES" = false ]; then
  echo "This will stop the Airflow stack (ECS, RDS, jumpbox)."
  echo "Use ./scripts/aws/start_stack.sh to bring it back."
  echo "Press Enter to continue or Ctrl-C to abort."
  read -r
fi

# 1. Scale ECS services to 0
echo "==> Scaling ECS services to 0..."
for svc in api-server scheduler dag-processor worker triggerer; do
  aws ecs update-service \
    --cluster "$CLUSTER" \
    --service "${PROJECT}-${svc}" \
    --desired-count 0 \
    --region "$REGION" \
    --no-cli-pager > /dev/null 2>&1 || true
  echo "    ${PROJECT}-${svc} → 0"
done

# 2. Stop RDS
echo "==> Stopping RDS instance..."
RDS_ID=$(aws rds describe-db-instances --region "$REGION" \
  --query "DBInstances[?DBInstanceIdentifier=='${PROJECT}-db'].DBInstanceIdentifier" \
  --output text 2>/dev/null)
if [ -n "$RDS_ID" ] && [ "$RDS_ID" != "None" ]; then
  RDS_STATUS=$(aws rds describe-db-instances --region "$REGION" \
    --db-instance-identifier "$RDS_ID" \
    --query 'DBInstances[0].DBInstanceStatus' --output text)
  if [ "$RDS_STATUS" = "available" ]; then
    aws rds stop-db-instance --db-instance-identifier "$RDS_ID" --region "$REGION" --no-cli-pager > /dev/null
    echo "    RDS ${RDS_ID} stopping... (note: AWS auto-restarts after 7 days)"
  else
    echo "    RDS ${RDS_ID} is ${RDS_STATUS}, skipping."
  fi
else
  echo "    No RDS instance found, skipping."
fi

# 3. Optionally delete ElastiCache (cannot be stopped)
if [ "$DELETE_REDIS" = true ]; then
  echo "==> Deleting ElastiCache cluster (cannot be stopped)..."
  aws elasticache delete-cache-cluster \
    --cache-cluster-id "${PROJECT}-redis" \
    --region "$REGION" \
    --no-cli-pager > /dev/null 2>&1 || echo "    Already deleted or not found."
  echo "    Redis cluster deletion initiated. Will be recreated on start."
else
  echo "==> Keeping ElastiCache running (~\$12/mo). Use --delete-redis to remove it."
fi

# 4. Stop jumpbox
echo "==> Stopping jumpbox EC2..."
JUMPBOX_ID=$(aws ec2 describe-instances --region "$REGION" \
  --filters "Name=tag:Name,Values=${PROJECT}-jumpbox" "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].InstanceId' --output text 2>/dev/null)
if [ -n "$JUMPBOX_ID" ] && [ "$JUMPBOX_ID" != "None" ]; then
  aws ec2 stop-instances --instance-ids "$JUMPBOX_ID" --region "$REGION" --no-cli-pager > /dev/null
  echo "    Jumpbox ${JUMPBOX_ID} stopping... (EIP retained)"
else
  echo "    No running jumpbox found, skipping."
fi

echo ""
echo "==> Stack stopped."
echo "    To bring it back: ./scripts/aws/start_stack.sh"
echo ""
echo "    Still incurring costs:"
echo "      - ALB:         ~\$16/mo"
echo "      - NAT Gateway: ~\$32/mo"
if [ "$DELETE_REDIS" = false ]; then
  echo "      - ElastiCache: ~\$12/mo"
fi
echo "      - EIP:         free (attached to stopped instance)"
echo "      - EFS:         minimal"
