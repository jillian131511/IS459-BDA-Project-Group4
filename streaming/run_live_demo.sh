#!/usr/bin/env bash
# Runs ON the serving EC2 (infra/launch_serving_ec2.sh). Starts the consumer, then replays the official grading
# days (never used for learning) through Kafka; alerts appear on the dashboard's Live tab within seconds.
#
#   bash /opt/aml/streaming/run_live_demo.sh <bucket> [variant=HI-Small] [start=2022/09/08] [events=300000] [speed=600] [end=2022/09/11]
#
# speed 600 = ten minutes of dataset time per second, so a day of transfers takes about 2.5 minutes.
set -euo pipefail
BUCKET="${1:?usage: run_live_demo.sh <bucket> [variant] [start] [events] [speed]}"
VARIANT="${2:-HI-Small}"; START="${3:-2022/09/08}"; EVENTS="${4:-300000}"; SPEED="${5:-600}"; END="${6:-2022/09/11}"
cd "$(dirname "$0")"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
PY=$(command -v python3.11 || command -v python3)
pkill -f streaming/consumer.py 2>/dev/null || pkill -f "consumer.py --bucket" 2>/dev/null || true
rm -f out/alerts.jsonl out/stats.json
# a fresh consumer group each run, so the replay is scored from the first event
nohup "$PY" consumer.py --bucket "$BUCKET" --variant "$VARIANT" --group "demo-$(date +%s)" > /var/log/aml_consumer.log 2>&1 &
sleep 8
"$PY" producer.py --source "s3://$BUCKET/raw/transactions/variant=$VARIANT/${VARIANT}_Trans.csv" \
    --start "$START" --end "$END" --max-events "$EVENTS" --speed "$SPEED" | tee /var/log/aml_producer.log
sleep 20   # let the consumer drain the topic and flush its last micro-batch to S3
echo "alerts so far: $(wc -l < out/alerts.jsonl 2>/dev/null || echo 0)"; cat out/stats.json 2>/dev/null || true
