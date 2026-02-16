#!/usr/bin/env bash
# Destroy all AWS resources. Usage: ./scripts/aws/teardown.sh [--yes]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INFRA_DIR="$(cd "$SCRIPT_DIR/../../infra" && pwd)"

cd "$INFRA_DIR"

if [ "${1:-}" != "--yes" ]; then
  echo "This will DESTROY all sec-scraper AWS resources."
  echo "Run with --yes to confirm, or press Enter to continue."
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

echo "==> Done. All resources destroyed."
