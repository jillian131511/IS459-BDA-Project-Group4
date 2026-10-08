#!/usr/bin/env bash
# Deletes every AWS resource this project created (brief: "delete all AWS resources after your final presentation").
# Asks for confirmation first because the data cannot be recovered.
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/config.env
read -r -p "Delete bucket $BUCKET (ALL data), Glue jobs/crawler/database, state machine, EMR clusters? type DELETE: " ok
[ "$ok" = "DELETE" ] || { echo "aborted"; exit 1; }
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

for id in $(aws emr list-clusters --active --query "Clusters[?starts_with(Name,'aml-q2')].Id" --output text); do
  aws emr terminate-clusters --cluster-ids "$id" && echo "terminated EMR $id"; done
aws stepfunctions delete-state-machine --state-machine-arn \
  "arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE}" || true
aws glue delete-job --job-name "$ETL_JOB" || true
aws glue delete-job --job-name "$ATHENA_JOB" || true
aws glue delete-crawler --name "$CRAWLER" || true
aws glue delete-database --name aml || true
aws s3 rm "s3://$BUCKET" --recursive && aws s3api delete-bucket --bucket "$BUCKET" || true
echo "Also stop/terminate the ingestion + Kafka EC2 instances in the EC2 console (tag Project=aml-g2)."
