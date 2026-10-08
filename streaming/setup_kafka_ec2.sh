#!/usr/bin/env bash
# Single-node Apache Kafka (KRaft mode, no ZooKeeper) on an EC2 t3.medium (Amazon Linux 2023).
# Same approach as the Week 7 Kafka hands-on; 3 partitions so a consumer group can scale out to 3 consumers.
set -euo pipefail
KAFKA_VERSION=3.8.1; SCALA=2.13
sudo dnf install -y java-17-amazon-corretto-headless python3-pip >/dev/null
cd ~ && [ -d kafka ] || { curl -sSLO "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA}-${KAFKA_VERSION}.tgz"
  tar xzf "kafka_${SCALA}-${KAFKA_VERSION}.tgz" && mv "kafka_${SCALA}-${KAFKA_VERSION}" kafka; }
cd ~/kafka
export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
if [ ! -f /tmp/kraft-combined-logs/meta.properties ]; then
  bin/kafka-storage.sh format -t "$(bin/kafka-storage.sh random-uuid)" -c config/kraft/server.properties
fi
nohup bin/kafka-server-start.sh config/kraft/server.properties > ~/kafka.log 2>&1 &
sleep 10
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic aml-transactions --partitions 3 --replication-factor 1
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic aml-transactions
pip3 install --user kafka-python boto3 streamlit pandas plotly networkx
echo "Kafka ready on localhost:9092"
