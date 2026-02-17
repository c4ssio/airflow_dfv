#!/usr/bin/env bash
# Destroy all Terraform-managed AWS resources (everything except the jumpbox).
# Run this FROM the jumpbox, then run destroy_jumpbox.sh locally to remove the jumpbox itself.
#
# Usage: ./scripts/aws/teardown.sh [--yes]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INFRA_DIR="$(cd "$SCRIPT_DIR/../../infra" && pwd)"

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
    --region us-east-1 \
    --no-cli-pager > /dev/null 2>&1 || true
done
echo "    Services scaled down."

echo "==> Terraform destroy..."
terraform destroy -auto-approve

echo ""
echo "==> Terraform resources destroyed."
echo ""
echo "    The jumpbox is still running (it's not managed by Terraform)."
echo "    To destroy it, run locally: ./scripts/aws/destroy_jumpbox.sh"
