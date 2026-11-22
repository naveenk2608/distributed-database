#!/usr/bin/env bash
# Lists what deploy/aws could have left behind: in the region, EC2
# instances that are not terminated, EBS volumes, Elastic IPs, NAT gateways
# that are not deleted, VPCs, subnets, internet gateways, security groups
# and key pairs tagged project=dkv; globally, the S3 buckets, IAM roles and
# instance profiles named after the cluster. Exits 1 if any is left. Then,
# for reference, counts the same kinds in the region whatever their tags.
# down.sh runs it after terraform destroy.
#
#   deploy/aws/check-empty.sh [REGION NAME]   (default: the terraform variables)
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
if [ $# -ge 2 ]; then
  region=$1 name=$2
else
  terraform -chdir="$here" init -input=false > /dev/null
  region=$(echo var.region | terraform -chdir="$here" console | tr -d '"')
  name=$(echo var.name | terraform -chdir="$here" console | tr -d '"')
fi
tag=Name=tag:project,Values=dkv
ec2() { aws ec2 --region "$region" --output text "$@"; }
left=0
check() { # WHAT IDS
  if [ -z "$2" ]; then
    printf '%-46s none\n' "$1"
  else
    printf '%-46s %s\n' "$1" "$(echo "$2" | tr '\t\n' '  ')"
    left=1
  fi
}

echo "Tagged project=dkv in $region:"
check "EC2 instances (not terminated)" "$(ec2 describe-instances --filters "$tag" \
  Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped --query 'Reservations[].Instances[].InstanceId')"
check "EBS volumes" "$(ec2 describe-volumes --filters "$tag" --query 'Volumes[].VolumeId')"
check "Elastic IPs" "$(ec2 describe-addresses --filters "$tag" --query 'Addresses[].PublicIp')"
check "NAT gateways (not deleted)" "$(ec2 describe-nat-gateways --filter "$tag" \
  Name=state,Values=pending,available,deleting,failed --query 'NatGateways[].NatGatewayId')"
check "VPCs" "$(ec2 describe-vpcs --filters "$tag" --query 'Vpcs[].VpcId')"
check "Subnets" "$(ec2 describe-subnets --filters "$tag" --query 'Subnets[].SubnetId')"
check "Internet gateways" "$(ec2 describe-internet-gateways --filters "$tag" --query 'InternetGateways[].InternetGatewayId')"
check "Security groups" "$(ec2 describe-security-groups --filters "$tag" --query 'SecurityGroups[].GroupId')"
check "Key pairs" "$(ec2 describe-key-pairs --filters "$tag" --query 'KeyPairs[].KeyName')"
echo "Named after the cluster (global):"
check "S3 buckets $name-artifacts-*" "$(aws s3api list-buckets --output text \
  --query "Buckets[?starts_with(Name, '$name-artifacts-')].Name")"
check "IAM roles $name-*" "$(aws iam list-roles --output text --query "Roles[?starts_with(RoleName, '$name-')].RoleName")"
check "IAM instance profiles $name-*" "$(aws iam list-instance-profiles --output text \
  --query "InstanceProfiles[?starts_with(InstanceProfileName, '$name-')].InstanceProfileName")"

count() { wc -w <<< "$1" | tr -d ' '; }
echo "In $region whatever the tags: $(count "$(ec2 describe-instances --filters \
  Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped \
  --query 'Reservations[].Instances[].InstanceId')") instances not terminated, $(count "$(ec2 describe-volumes \
  --query 'Volumes[].VolumeId')") volumes, $(count "$(ec2 describe-addresses --query 'Addresses[].PublicIp')") Elastic IPs, $(count \
  "$(ec2 describe-nat-gateways --filter Name=state,Values=pending,available,deleting,failed \
    --query 'NatGateways[].NatGatewayId')") NAT gateways, $(count "$(ec2 describe-vpcs --filters Name=is-default,Values=false \
  --query 'Vpcs[].VpcId')") VPCs besides the default one"
if [ "$left" -ne 0 ]; then
  echo "Something tagged project=dkv or named $name-* is left (above)." >&2
  exit 1
fi
echo "Nothing tagged project=dkv or named $name-* is left."
