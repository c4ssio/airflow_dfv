#!/usr/bin/env bash
# Destroy all Terraform-managed AWS resources (everything except the jumpbox).
# Run this FROM the jumpbox, then run destroy_jumpbox.sh locally to remove the jumpbox itself.
#
# Creates a timestamped RDS snapshot before destroying (named with YYYYMMDD-HHMM).
# The start_stack.sh / deploy.sh scripts will auto-detect the latest snapshot.
#
# Usage: ./scripts/aws/teardown.sh [--yes] [--skip-snapshot]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INFRA_DIR="$(cd "$SCRIPT_DIR/../../infra" && pwd)"
REGION="us-east-1"
PROJECT="sec-scraper"
SKIP_SNAPSHOT=false

for arg in "$@"; do
  case $arg in
    --skip-snapshot) SKIP_SNAPSHOT=true ;;
  esac
done

cd "$INFRA_DIR"

if [ "${1:-}" != "--yes" ]; then
  echo "This will DESTROY all sec-scraper AWS resources (VPC, ECS, RDS, etc.)."
  echo "The jumpbox is NOT managed by Terraform — destroy it separately with destroy_jumpbox.sh."
  echo ""
  echo "Run with --yes to skip this prompt, or press Enter to continue."
  read -r
fi

echo "==> Scaling ECS services to 0..."
CLUSTER=$(terraform output -raw ecs_cluster_name 2>/dev/null || echo "sec-scraper-cluster")
for svc in api-server scheduler dag-processor worker triggerer; do
  aws ecs update-service \
    --cluster "$CLUSTER" \
    --service "sec-scraper-${svc}" \
    --desired-count 0 \
    --region "$REGION" \
    --no-cli-pager > /dev/null 2>&1 || true
done
echo "    Services scaled down."

# --- Create RDS snapshot before destroying ---
if [ "$SKIP_SNAPSHOT" = false ]; then
  echo "==> Creating RDS snapshot before teardown..."
  RDS_ID="${PROJECT}-db"
  TIMESTAMP=$(date -u +"%Y%m%d-%H%M")
  SNAPSHOT_ID="${PROJECT}-db-pre-teardown-${TIMESTAMP}"

  RDS_STATUS=$(aws rds describe-db-instances --region "$REGION" \
    --db-instance-identifier "$RDS_ID" \
    --query 'DBInstances[0].DBInstanceStatus' --output text 2>/dev/null || echo "not-found")

  if [ "$RDS_STATUS" = "not-found" ]; then
    echo "    No RDS instance found, skipping snapshot."
  elif [ "$RDS_STATUS" = "stopped" ]; then
    echo "    RDS is stopped. Starting it to take snapshot..."
    aws rds start-db-instance --db-instance-identifier "$RDS_ID" --region "$REGION" --no-cli-pager > /dev/null
    echo "    Waiting for RDS to become available..."
    aws rds wait db-instance-available --db-instance-identifier "$RDS_ID" --region "$REGION"
    echo "    RDS is available. Creating snapshot: ${SNAPSHOT_ID}"
    aws rds create-db-snapshot \
      --db-instance-identifier "$RDS_ID" \
      --db-snapshot-identifier "$SNAPSHOT_ID" \
      --region "$REGION" \
      --no-cli-pager > /dev/null
    echo "    Waiting for snapshot to complete..."
    aws rds wait db-snapshot-available \
      --db-snapshot-identifier "$SNAPSHOT_ID" \
      --region "$REGION"
    echo "    Snapshot created: ${SNAPSHOT_ID}"
  elif [ "$RDS_STATUS" = "available" ]; then
    echo "    Creating snapshot: ${SNAPSHOT_ID}"
    aws rds create-db-snapshot \
      --db-instance-identifier "$RDS_ID" \
      --db-snapshot-identifier "$SNAPSHOT_ID" \
      --region "$REGION" \
      --no-cli-pager > /dev/null
    echo "    Waiting for snapshot to complete..."
    aws rds wait db-snapshot-available \
      --db-snapshot-identifier "$SNAPSHOT_ID" \
      --region "$REGION"
    echo "    Snapshot created: ${SNAPSHOT_ID}"
  else
    echo "    RDS status is ${RDS_STATUS}, cannot snapshot. Skipping."
  fi
else
  echo "==> Skipping RDS snapshot (--skip-snapshot)."
fi

echo "==> Terraform destroy..."
terraform destroy -auto-approve

echo ""
echo "==> Terraform resources destroyed."
echo ""
echo "    The jumpbox is still running (it's not managed by Terraform)."
echo "    To destroy it, run locally: ./scripts/aws/destroy_jumpbox.sh"
if [ "$SKIP_SNAPSHOT" = false ] && [ "${SNAPSHOT_ID:-}" != "" ]; then
  echo ""
  echo "    RDS snapshot preserved: ${SNAPSHOT_ID}"
  echo "    Next deploy will auto-detect the latest snapshot."
fi
