#!/usr/bin/env bash
# Deletes every AWS resource this project created (brief: "delete all AWS resources after your final presentation").
# Asks for confirmation first because the data cannot be recovered.
#
# Order matters: EC2 instances go before their security group and before the instance profile they use.
# Deliberately NOT deleted: the account-wide EMR default roles (EMR_DefaultRole, EMR_EC2_DefaultRole), which
# other EMR users in the account may rely on, and any role that create_roles.sh did not create (e.g. LabRole).
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/config.env
ROLE="${PIPELINE_ROLE:-aml-pipeline-role}"
PROFILE="${INSTANCE_PROFILE:-aml-pipeline-profile}"
SG_NAME=aml-serving
read -r -p "Delete bucket $BUCKET (ALL data), Glue jobs/crawler/database, state machine, EMR clusters, \
EC2 instances tagged Project=aml-g2, security group $SG_NAME and IAM role $ROLE? type DELETE: " ok
[ "$ok" = "DELETE" ] || { echo "aborted"; exit 1; }
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# 1. pipeline: EMR, Step Functions, Glue, S3
for id in $(aws emr list-clusters --active --query "Clusters[?starts_with(Name,'aml-q2')].Id" --output text); do
  aws emr terminate-clusters --cluster-ids "$id" && echo "terminated EMR $id"; done
aws stepfunctions delete-state-machine --state-machine-arn \
  "arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE}" || true
aws glue delete-job --job-name "$ETL_JOB" || true
aws glue delete-job --job-name "$ATHENA_JOB" || true
aws glue delete-crawler --name "$CRAWLER" || true
aws glue delete-database --name aml || true
aws s3 rm "s3://$BUCKET" --recursive && aws s3api delete-bucket --bucket "$BUCKET" || true

# 2. EC2: the serving machine (Kafka + dashboard) and any ingestion machine still alive
IIDS=$(aws ec2 describe-instances \
  --filters Name=tag:Project,Values=aml-g2 Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query "Reservations[].Instances[].InstanceId" --output text)
if [ -n "$IIDS" ]; then
  aws ec2 terminate-instances --instance-ids $IIDS >/dev/null
  echo "terminating EC2 $IIDS (waiting, so the security group and instance profile are free to delete)"
  aws ec2 wait instance-terminated --instance-ids $IIDS
  echo "terminated EC2 $IIDS"
fi

# 3. the dashboard's security group (only after its instance is gone)
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text)
SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=$SG_NAME Name=vpc-id,Values=$VPC \
     --query "SecurityGroups[0].GroupId" --output text 2>/dev/null || echo None)
if [ "$SG" != "None" ] && [ -n "$SG" ]; then
  aws ec2 delete-security-group --group-id "$SG" && echo "deleted security group $SG ($SG_NAME)" || true
fi

# 4. IAM: only the role and instance profile that create_roles.sh made (tagged Project=aml-g2)
ROLE_TAG=$(aws iam list-role-tags --role-name "$ROLE" --query "Tags[?Key=='Project'].Value" --output text 2>/dev/null || true)
if [ "$ROLE_TAG" = "aml-g2" ]; then
  if aws iam get-instance-profile --instance-profile-name "$PROFILE" >/dev/null 2>&1; then
    aws iam remove-role-from-instance-profile --instance-profile-name "$PROFILE" --role-name "$ROLE" || true
    aws iam delete-instance-profile --instance-profile-name "$PROFILE" && echo "deleted instance profile $PROFILE"
  fi
  for arn in $(aws iam list-attached-role-policies --role-name "$ROLE" --query "AttachedPolicies[].PolicyArn" --output text); do
    aws iam detach-role-policy --role-name "$ROLE" --policy-arn "$arn"; done
  for name in $(aws iam list-role-policies --role-name "$ROLE" --query "PolicyNames[]" --output text); do
    aws iam delete-role-policy --role-name "$ROLE" --policy-name "$name"; done
  aws iam delete-role --role-name "$ROLE" && echo "deleted IAM role $ROLE"
else
  echo "IAM role $ROLE not deleted: it was not created by create_roles.sh (no Project=aml-g2 tag), e.g. Learner Lab's LabRole."
fi

echo "Done. Delete any IAM access keys made for the project by hand (IAM -> Users -> Security credentials)."
