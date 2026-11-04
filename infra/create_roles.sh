#!/usr/bin/env bash
# =====================================================================================================
# Creates the IAM role the pipeline runs as, for a normal AWS account (NOT needed in AWS Academy Learner
# Lab, which already provides LabRole + LabInstanceProfile). Idempotent - safe to re-run.
#
#   bash infra/create_roles.sh            # then set PIPELINE_ROLE / INSTANCE_PROFILE in infra/config.env
#
# One role, trusted by the three services that act on our behalf:
#   glue.amazonaws.com     Glue ETL job, crawler, Athena-runner job
#   states.amazonaws.com   Step Functions (starts Glue, creates/terminates the EMR cluster)
#   ec2.amazonaws.com      the throw-away ingestion instance (via the instance profile)
# Also tags the default-VPC subnets so EMR's v2 managed service role is allowed to launch into them.
# =====================================================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/config.env
ROLE="${PIPELINE_ROLE:-aml-pipeline-role}"
PROFILE="${INSTANCE_PROFILE:-aml-pipeline-profile}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":["glue.amazonaws.com","states.amazonaws.com","ec2.amazonaws.com"]},"Action":"sts:AssumeRole"}]}'
if aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  aws iam update-assume-role-policy --role-name "$ROLE" --policy-document "$TRUST"
else
  aws iam create-role --role-name "$ROLE" --assume-role-policy-document "$TRUST" \
    --description "IS459 G2 Grp4 AML pipeline (Glue, Step Functions, ingestion EC2)" \
    --tags Key=Project,Value=aml-g2 --query Role.Arn --output text
fi

for P in service-role/AWSGlueServiceRole AmazonS3FullAccess AmazonAthenaFullAccess \
         AmazonEMRFullAccessPolicy_v2 CloudWatchLogsFullAccess AmazonEventBridgeFullAccess; do
  aws iam attach-role-policy --role-name "$ROLE" --policy-arn "arn:aws:iam::aws:policy/$P"
done
# Step Functions must be allowed to hand the EMR roles (and this role, for Glue) to the services it starts
aws iam put-role-policy --role-name "$ROLE" --policy-name pass-pipeline-roles --policy-document "{
  \"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"iam:PassRole\",\"Resource\":[
    \"arn:aws:iam::${ACCOUNT_ID}:role/${EMR_SERVICE_ROLE}\",\"arn:aws:iam::${ACCOUNT_ID}:role/${EMR_EC2_PROFILE}\",
    \"arn:aws:iam::${ACCOUNT_ID}:role/${ROLE}\"]}]}"

aws iam get-instance-profile --instance-profile-name "$PROFILE" >/dev/null 2>&1 || \
  aws iam create-instance-profile --instance-profile-name "$PROFILE" --query InstanceProfile.Arn --output text
aws iam get-instance-profile --instance-profile-name "$PROFILE" --query "InstanceProfile.Roles[].RoleName" \
  --output text | grep -qw "$ROLE" || aws iam add-role-to-instance-profile --instance-profile-name "$PROFILE" --role-name "$ROLE"

# EMR default roles (created once per account); harmless if they already exist
aws iam get-role --role-name "$EMR_SERVICE_ROLE" >/dev/null 2>&1 || aws emr create-default-roles >/dev/null

# AmazonEMRServicePolicy_v2 only launches into subnets tagged for EMR-managed policies
SUBNETS=$(aws ec2 describe-subnets --filters Name=default-for-az,Values=true --query "Subnets[].SubnetId" --output text)
[ -n "$SUBNETS" ] && aws ec2 create-tags --resources $SUBNETS --tags Key=for-use-with-amazon-emr-managed-policies,Value=true

echo "role=$ROLE instance_profile=$PROFILE ready (IAM changes can take ~10 s to apply)"
