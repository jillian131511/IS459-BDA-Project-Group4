#!/usr/bin/env bash
# =====================================================================================================
# Launches a throw-away EC2 instance that downloads the chosen variants from Kaggle straight into the
# S3 raw zone (ingestion/download_to_s3.sh), uploads its log, then SHUTS ITSELF DOWN AND TERMINATES.
# Nothing is left running, and the data never passes through a laptop.
#
#   export KAGGLE_API_TOKEN=KGAT_...            # kaggle.com -> Settings -> API -> Create New Token
#   bash infra/launch_ingestion_ec2.sh HI-Small HI-Medium
#
# Progress: aws s3 cp s3://$BUCKET/raw/_ingest_logs/<instance-id>.log -
# =====================================================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/config.env
: "${KAGGLE_API_TOKEN:?export KAGGLE_API_TOKEN first}"
VARIANTS="${*:?usage: launch_ingestion_ec2.sh <variant> [variant ...]}"

# The token goes to the private bucket (public access blocked) for the instance to read once, then it
# is deleted - so it never appears in the instance's user data.
aws s3 cp ingestion/download_to_s3.sh "s3://$BUCKET/scripts/ingestion/download_to_s3.sh" --only-show-errors
printf '%s' "$KAGGLE_API_TOKEN" | aws s3 cp - "s3://$BUCKET/secrets/kaggle_token" --only-show-errors

AMI=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
      --query Parameter.Value --output text)

cat > /tmp/aml_ingest_userdata.sh <<EOF
#!/bin/bash
ID=\$(TOKEN=\$(curl -s -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 300"); \
     curl -s -H "X-aws-ec2-metadata-token: \$TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
LOG=/var/log/aml_ingest.log
exec > >(tee -a \$LOG) 2>&1
push_log() { aws s3 cp \$LOG s3://$BUCKET/raw/_ingest_logs/\$ID.log --only-show-errors || true; }
trap 'push_log; shutdown -h now' EXIT
echo "start \$(date -u +%FT%TZ) variants: $VARIANTS"
dnf install -y -q python3.11 python3.11-pip unzip
export HOME=/root BUCKET=$BUCKET AWS_DEFAULT_REGION=$AWS_REGION
export KAGGLE_API_TOKEN=\$(aws s3 cp s3://$BUCKET/secrets/kaggle_token -) && aws s3 rm s3://$BUCKET/secrets/kaggle_token
aws s3 cp s3://$BUCKET/scripts/ingestion/download_to_s3.sh /root/download_to_s3.sh
for V in $VARIANTS; do
  SECONDS=0; bash /root/download_to_s3.sh \$V && echo "OK \$V in \${SECONDS}s" || echo "FAILED \$V"
  push_log
done
echo "done \$(date -u +%FT%TZ)"
EOF

IID=$(aws ec2 run-instances --image-id "$AMI" --instance-type t3.medium \
  --iam-instance-profile Name="${INSTANCE_PROFILE:-LabInstanceProfile}" \
  --instance-initiated-shutdown-behavior terminate \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=60,VolumeType=gp3,DeleteOnTermination=true}' \
  --user-data file:///tmp/aml_ingest_userdata.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=aml-ingest},{Key=Project,Value=aml-g2}]' \
  --query 'Instances[0].InstanceId' --output text)
echo "launched $IID - it terminates itself when finished."
echo "progress: aws s3 cp s3://$BUCKET/raw/_ingest_logs/$IID.log -"
