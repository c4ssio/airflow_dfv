#!/usr/bin/env bash
# Destroy the standalone jumpbox created by create_jumpbox.sh.
# Reads state from ~/.sec-scraper-jumpbox.json.
#
# Usage:
#   ./scripts/aws/destroy_jumpbox.sh          # Interactive confirm
#   ./scripts/aws/destroy_jumpbox.sh --yes    # No prompt
set -euo pipefail

STATE_FILE="$HOME/.sec-scraper-jumpbox.json"
AUTO_YES=false

for arg in "$@"; do
  case $arg in
    --yes) AUTO_YES=true ;;
  esac
done

if [ ! -f "$STATE_FILE" ]; then
  echo "No jumpbox state file found at $STATE_FILE."
  echo "Nothing to destroy (jumpbox may have been created manually or already destroyed)."
  exit 0
fi

# Parse state
INSTANCE_ID=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['instance_id'])")
SG_ID=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['security_group_id'])")
EIP_ALLOC=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['eip_allocation_id'])")
KEY_NAME=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['key_name'])")
CREATED_KEY=$(python3 -c "import json; print(str(json.load(open('$STATE_FILE')).get('created_key', False)).lower())")
KEY_FILE=$(python3 -c "import json; print(json.load(open('$STATE_FILE')).get('key_file', ''))")
REGION=$(python3 -c "import json; print(json.load(open('$STATE_FILE'))['region'])")

echo "Will destroy jumpbox:"
echo "  Instance:       $INSTANCE_ID"
echo "  Security group: $SG_ID"
echo "  Elastic IP:     $EIP_ALLOC"
if [ "$CREATED_KEY" = "true" ]; then
  echo "  Key pair:       $KEY_NAME (will be deleted)"
fi
echo ""

if [ "$AUTO_YES" = false ]; then
  echo "Press Enter to continue or Ctrl-C to abort."
  read -r
fi

# 1. Terminate instance
echo "==> Terminating instance $INSTANCE_ID..."
aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$REGION" > /dev/null 2>&1 || true

echo "    Waiting for termination..."
aws ec2 wait instance-terminated --instance-ids "$INSTANCE_ID" --region "$REGION" 2>/dev/null || sleep 30

# 2. Release Elastic IP
echo "==> Releasing Elastic IP $EIP_ALLOC..."
aws ec2 release-address --allocation-id "$EIP_ALLOC" --region "$REGION" 2>/dev/null || true

# 3. Delete security group (may need a retry after instance is fully gone)
echo "==> Deleting security group $SG_ID..."
for i in 1 2 3; do
  if aws ec2 delete-security-group --group-id "$SG_ID" --region "$REGION" 2>/dev/null; then
    break
  fi
  echo "    Retry $i (waiting for instance to fully terminate)..."
  sleep 10
done

# 4. Delete key pair if we created it
if [ "$CREATED_KEY" = "true" ]; then
  echo "==> Deleting key pair $KEY_NAME..."
  aws ec2 delete-key-pair --key-name "$KEY_NAME" --region "$REGION" 2>/dev/null || true
  if [ -n "$KEY_FILE" ] && [ -f "$KEY_FILE" ]; then
    rm -f "$KEY_FILE"
    echo "    Deleted local key file: $KEY_FILE"
  fi
fi

# 5. Remove state file
rm -f "$STATE_FILE"
echo ""
echo "==> Jumpbox destroyed. State file removed."
