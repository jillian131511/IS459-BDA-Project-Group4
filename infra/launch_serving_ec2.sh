#!/usr/bin/env bash
# =====================================================================================================
# The SERVING machine: one EC2 (t3.large, Amazon Linux 2023) that runs
#   * Apache Kafka 3.8.1 (KRaft, single node) with topic aml-transactions (3 partitions)   - speed layer
#   * the Streamlit dashboard in Athena mode on port 8501                                   - visualisation
# both as systemd services, so they come back by themselves after a stop/start.
# The producer and consumer are started on demand (see streaming/run_live_demo.sh).
#
#   bash infra/launch_serving_ec2.sh            # prints the dashboard URL when the machine is ready
#   aws ec2 stop-instances  --instance-ids <id> # when nobody is using it (costs ~USD 0.08/hour while running)
#   aws ec2 start-instances --instance-ids <id> # services restart on boot; the public IP changes
#
# Managed through AWS Systems Manager (no SSH key, no port 22). Port 8501 is opened to DASHBOARD_CIDR
# (default 0.0.0.0/0: the dashboard only shows the public synthetic Kaggle data).
# =====================================================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/config.env
CIDR="${DASHBOARD_CIDR:-0.0.0.0/0}"
NAME=aml-serving

# 1. the code, packaged from this checkout
tar czf /tmp/aml_repo.tgz --exclude=.git --exclude=data --exclude='__pycache__' --exclude=infra/config.env .
aws s3 cp /tmp/aml_repo.tgz "s3://$BUCKET/scripts/serving/repo.tgz" --only-show-errors

# 2. a security group that lets browsers reach the dashboard
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text)
SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=$NAME Name=vpc-id,Values=$VPC \
     --query "SecurityGroups[0].GroupId" --output text)
if [ "$SG" = "None" ]; then
  SG=$(aws ec2 create-security-group --group-name $NAME --description "AML dashboard (Streamlit 8501)" \
       --vpc-id "$VPC" --query GroupId --output text)
  aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 8501 --cidr "$CIDR" >/dev/null
  aws ec2 create-tags --resources "$SG" --tags Key=Project,Value=aml-g2
fi

# 3. what the machine does on first boot
cat > /tmp/aml_serving_userdata.sh <<EOF
#!/bin/bash
exec > >(tee -a /var/log/aml_serving.log) 2>&1
set -x
dnf install -y -q python3.11 python3.11-pip java-17-amazon-corretto-headless
mkdir -p /opt/aml && cd /opt/aml
aws s3 cp s3://$BUCKET/scripts/serving/repo.tgz /tmp/repo.tgz --region $AWS_REGION && tar xzf /tmp/repo.tgz
python3.11 -m pip install -q "streamlit==1.66.1" "plotly>=5.18" "pandas>=2.0" numpy networkx boto3 "kafka-python>=2.0.3"
cd /opt && curl -sSLO https://archive.apache.org/dist/kafka/3.8.1/kafka_2.13-3.8.1.tgz && tar xzf kafka_2.13-3.8.1.tgz && mv kafka_2.13-3.8.1 kafka
/opt/kafka/bin/kafka-storage.sh format -t \$(/opt/kafka/bin/kafka-storage.sh random-uuid) -c /opt/kafka/config/kraft/server.properties
cat > /etc/systemd/system/kafka.service <<'UNIT'
[Unit]
Description=Apache Kafka (KRaft, single node)
After=network.target
[Service]
Environment=KAFKA_HEAP_OPTS=-Xmx1G -Xms1G
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
Restart=on-failure
[Install]
WantedBy=multi-user.target
UNIT
cat > /etc/systemd/system/aml-dashboard.service <<'UNIT'
[Unit]
Description=AML Streamlit dashboard (Athena mode)
After=network.target
[Service]
WorkingDirectory=/opt/aml
Environment=DATA_MODE=athena BUCKET=$BUCKET AWS_REGION=$AWS_REGION AWS_DEFAULT_REGION=$AWS_REGION
ExecStart=/usr/bin/python3.11 -m streamlit run dashboard/app.py --server.port 8501 --server.headless true --browser.gatherUsageStats false
Restart=on-failure
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload && systemctl enable --now kafka aml-dashboard
for i in \$(seq 1 30); do /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break; sleep 5; done
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic aml-transactions --partitions 3 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic aml-transactions
echo SERVING_READY
EOF

AMI=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
      --query Parameter.Value --output text)
IID=$(aws ec2 run-instances --image-id "$AMI" --instance-type t3.large \
  --iam-instance-profile Name="${INSTANCE_PROFILE:-LabInstanceProfile}" --security-group-ids "$SG" \
  --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=30,VolumeType=gp3,DeleteOnTermination=true}' \
  --user-data file:///tmp/aml_serving_userdata.sh \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME},{Key=Project,Value=aml-g2}]" \
  --query 'Instances[0].InstanceId' --output text)
echo "launched $IID - first boot installs Kafka + the dashboard (about 3-5 minutes)"
aws ec2 wait instance-running --instance-ids "$IID"
IP=$(aws ec2 describe-instances --instance-ids "$IID" --query "Reservations[0].Instances[0].PublicIpAddress" --output text)
echo "dashboard: http://$IP:8501   (ready a few minutes after boot)"
