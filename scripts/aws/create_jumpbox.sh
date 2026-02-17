#!/usr/bin/env bash
# Create a standalone jumpbox EC2 instance for running Terraform and deploy operations.
# This does NOT require Terraform — only AWS CLI.
#
# The jumpbox is created in the default VPC with a security group allowing SSH from
# your current IP. It installs Terraform, Docker, Git, and SSM Session Manager.
#
# Usage:
#   ./scripts/aws/create_jumpbox.sh                    # Auto-detect your IP
#   ./scripts/aws/create_jumpbox.sh --ip 1.2.3.4/32    # Explicit IP/CIDR
#   ./scripts/aws/create_jumpbox.sh --key my-key        # Use existing key pair
#
# State is saved to ~/.sec-scraper-jumpbox.json for destroy_jumpbox.sh.
set -euo pipefail

REGION="us-east-1"
PROJECT="sec-scraper"
STATE_FILE="$HOME/.sec-scraper-jumpbox.json"
INSTANCE_TYPE="t3.micro"
MY_IP=""
KEY_NAME=""

for arg in "$@"; do
  case $arg in
    --ip)       shift; MY_IP="$1"; shift ;;
    --ip=*)     MY_IP="${arg#*=}" ;;
    --key)      shift; KEY_NAME="$1"; shift ;;
    --key=*)    KEY_NAME="${arg#*=}" ;;
  esac
done 2>/dev/null || true

# --- Preflight checks ---
if ! command -v aws &>/dev/null; then
  echo "ERROR: AWS CLI not found. Install it first." >&2
  exit 1
fi

if [ -f "$STATE_FILE" ]; then
  echo "ERROR: $STATE_FILE already exists. A jumpbox may already be running."
  echo "       Run destroy_jumpbox.sh first, or delete the state file if stale."
  exit 1
fi

# Verify credentials
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION" 2>&1) || {
  echo "ERROR: AWS credentials not configured. Export AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_DEFAULT_REGION." >&2
  exit 1
}
echo "Account: $ACCOUNT_ID | Region: $REGION"

# --- Detect caller's IP ---
if [ -z "$MY_IP" ]; then
  echo "Detecting your public IP..."
  MY_IP=$(curl -s --max-time 5 https://checkip.amazonaws.com | tr -d '\n')
  if [ -z "$MY_IP" ]; then
    echo "ERROR: Could not detect public IP. Use --ip YOUR_IP/32" >&2
    exit 1
  fi
  MY_IP="${MY_IP}/32"
fi
echo "SSH access from: $MY_IP"

# --- Find default VPC ---
echo "Finding default VPC..."
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=is-default,Values=true" \
  --query 'Vpcs[0].VpcId' --output text --region "$REGION")
if [ -z "$VPC_ID" ] || [ "$VPC_ID" = "None" ]; then
  echo "ERROR: No default VPC found in $REGION. Create one with: aws ec2 create-default-vpc" >&2
  exit 1
fi
echo "Using default VPC: $VPC_ID"

# Pick the first public subnet in the default VPC
SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" \
  --query 'Subnets[?MapPublicIpOnLaunch==`true`] | [0].SubnetId' --output text --region "$REGION")
if [ -z "$SUBNET_ID" ] || [ "$SUBNET_ID" = "None" ]; then
  # Fallback: just use the first subnet
  SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" \
    --query 'Subnets[0].SubnetId' --output text --region "$REGION")
fi
echo "Subnet: $SUBNET_ID"

# --- Create security group ---
echo "Creating security group..."
SG_ID=$(aws ec2 create-security-group \
  --group-name "${PROJECT}-jumpbox-sg" \
  --description "Jumpbox SSH from deploy machine" \
  --vpc-id "$VPC_ID" \
  --region "$REGION" \
  --query 'GroupId' --output text 2>&1) || {
  # SG might already exist from a partial create
  SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=${PROJECT}-jumpbox-sg" "Name=vpc-id,Values=$VPC_ID" \
    --query 'SecurityGroups[0].GroupId' --output text --region "$REGION")
}

aws ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" \
  --protocol tcp --port 22 \
  --cidr "$MY_IP" \
  --region "$REGION" 2>/dev/null || true
echo "Security group: $SG_ID (SSH from $MY_IP)"

# --- Key pair ---
CREATED_KEY=false
if [ -z "$KEY_NAME" ]; then
  KEY_NAME="${PROJECT}-jumpbox-key"
  KEY_FILE="$HOME/.ssh/${KEY_NAME}.pem"
  # Check if key pair exists in AWS
  if aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$REGION" &>/dev/null; then
    echo "Using existing key pair: $KEY_NAME"
    if [ ! -f "$KEY_FILE" ]; then
      echo "WARNING: Key pair exists in AWS but $KEY_FILE not found locally."
      echo "         You may not be able to SSH in. Delete the key pair and rerun, or use --key."
    fi
  else
    echo "Creating key pair: $KEY_NAME"
    mkdir -p "$HOME/.ssh"
    aws ec2 create-key-pair --key-name "$KEY_NAME" --region "$REGION" \
      --query 'KeyMaterial' --output text > "$KEY_FILE"
    chmod 600 "$KEY_FILE"
    CREATED_KEY=true
    echo "Private key saved: $KEY_FILE"
  fi
else
  KEY_FILE="$HOME/.ssh/${KEY_NAME}.pem"
  echo "Using key pair: $KEY_NAME"
fi

# --- Get latest Amazon Linux 2023 AMI ---
echo "Finding latest Amazon Linux 2023 AMI..."
AMI_ID=$(aws ssm get-parameter \
  --name "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64" \
  --query 'Parameter.Value' --output text --region "$REGION")
echo "AMI: $AMI_ID"

# --- User data script ---
USER_DATA=$(cat <<'USERDATA'
#!/bin/bash
set -ex
dnf install -y git docker
dnf config-manager --add-repo https://rpm.releases.hashicorp.com/AmazonLinux/hashicorp.repo || \
  yum-config-manager --add-repo https://rpm.releases.hashicorp.com/AmazonLinux/hashicorp.repo
dnf install -y terraform || yum install -y terraform
curl -o /tmp/session-manager-plugin.rpm \
  "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/linux_64bit/session-manager-plugin.rpm"
dnf install -y /tmp/session-manager-plugin.rpm || yum install -y /tmp/session-manager-plugin.rpm
systemctl enable docker && systemctl start docker
usermod -aG docker ec2-user
su - ec2-user -c "mkdir -p ~/projects"
echo "JUMPBOX_SETUP_COMPLETE" > /tmp/setup-complete
USERDATA
)

# --- Launch instance ---
echo "Launching $INSTANCE_TYPE instance..."
INSTANCE_ID=$(aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_NAME" \
  --security-group-ids "$SG_ID" \
  --subnet-id "$SUBNET_ID" \
  --associate-public-ip-address \
  --user-data "$USER_DATA" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${PROJECT}-jumpbox},{Key=Environment,Value=ephemeral}]" \
  --region "$REGION" \
  --query 'Instances[0].InstanceId' --output text)
echo "Instance: $INSTANCE_ID"

# --- Allocate and associate Elastic IP ---
echo "Allocating Elastic IP..."
EIP_ALLOC=$(aws ec2 allocate-address --domain vpc --region "$REGION" \
  --tag-specifications "ResourceType=elastic-ip,Tags=[{Key=Name,Value=${PROJECT}-jumpbox-eip},{Key=Environment,Value=ephemeral}]" \
  --query 'AllocationId' --output text)
echo "EIP allocation: $EIP_ALLOC"

echo "Waiting for instance to be running..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$REGION"

echo "Associating Elastic IP..."
aws ec2 associate-address --instance-id "$INSTANCE_ID" --allocation-id "$EIP_ALLOC" --region "$REGION" > /dev/null

PUBLIC_IP=$(aws ec2 describe-addresses --allocation-ids "$EIP_ALLOC" --region "$REGION" \
  --query 'Addresses[0].PublicIp' --output text)

# --- Save state ---
cat > "$STATE_FILE" <<STATEJSON
{
  "instance_id": "$INSTANCE_ID",
  "security_group_id": "$SG_ID",
  "eip_allocation_id": "$EIP_ALLOC",
  "public_ip": "$PUBLIC_IP",
  "key_name": "$KEY_NAME",
  "key_file": "$KEY_FILE",
  "created_key": $CREATED_KEY,
  "region": "$REGION",
  "vpc_id": "$VPC_ID"
}
STATEJSON
echo "State saved: $STATE_FILE"

echo ""
echo "============================================"
echo "  Jumpbox created successfully!"
echo "============================================"
echo ""
echo "  IP:   $PUBLIC_IP"
echo "  SSH:  ssh -i $KEY_FILE ec2-user@$PUBLIC_IP"
echo ""
echo "  The instance is installing Terraform, Docker, and Git."
echo "  Wait ~2 minutes, then SSH in and run:"
echo ""
echo "    git clone <your-repo-url> ~/projects/airflow_dfv"
echo "    cd ~/projects/airflow_dfv/infra"
echo "    cp terraform.tfvars.example terraform.tfvars"
echo "    # Edit terraform.tfvars (set allowed_cidr, etc.)"
echo "    ../scripts/aws/deploy.sh"
echo ""
echo "  To tear down later:"
echo "    # On the jumpbox: ./scripts/aws/teardown.sh --yes"
echo "    # Locally:        ./scripts/aws/destroy_jumpbox.sh"
echo ""
