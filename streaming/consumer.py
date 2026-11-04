"""
Kafka CONSUMER - real-time AML alerting (the 'speed layer').

Loads the batch layer's outputs (Q2 watchlist, Q1 segment risk, FX rates) from the serving CSVs written by the
EMR job, consumes `aml-transactions`, scores every event with streaming/monitor_core.py and writes alerts:
  * locally  -> streaming/out/alerts.jsonl + stats.json   (the dashboard's Live tab tails these)
  * to S3    -> s3://BUCKET/streaming/alerts/variant=<v>/  every --flush-secs (durable, queryable later)

  python streaming/consumer.py --bucket BUCKET --variant HI-Small --bootstrap localhost:9092
Run 2+ copies with the same --group to see Kafka spread the partitions across consumers (consumer group).
"""
import argparse
import json
import os
import statistics
import time

import boto3
from kafka import KafkaConsumer

from monitor_core import RiskModel

OUT = os.path.join(os.path.dirname(__file__), "out")


def read_serving_csv(s3, bucket, prefix):
    """Spark writes one part-*.csv per folder - find it and return its text."""
    for o in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", []):
        if o["Key"].endswith(".csv"):
            return s3.get_object(Bucket=bucket, Key=o["Key"])["Body"].read().decode()
    raise FileNotFoundError(f"s3://{bucket}/{prefix}*.csv - run the pipeline first")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--variant", default="HI-Small")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="aml-transactions")
    ap.add_argument("--group", default="aml-monitor")
    ap.add_argument("--min-probability", type=float, default=0.5)
    # 11 = chosen on a LEARNING day (7 Sep replay, hubs excluded) and equal to the batch layer's fan_out_k; see
    # docs/decisions.md. The grading day (8 Sep) was measured once, after the choice.
    ap.add_argument("--velocity-k", type=int, default=11, help="distinct payees in 24 h that raise a velocity alert")
    ap.add_argument("--flush-secs", type=int, default=15)
    a = ap.parse_args()

    s3 = boto3.client("s3")
    base = f"analytics/serving/variant={a.variant}"
    try:  # hub accounts from the batch layer (older runs did not publish them)
        hubs = [l.strip().strip('"') for l in read_serving_csv(s3, a.bucket, f"{base}/hubs/").splitlines()[1:]]
    except FileNotFoundError:
        hubs = []
    model = RiskModel(read_serving_csv(s3, a.bucket, f"{base}/watchlist/"),
                      read_serving_csv(s3, a.bucket, f"{base}/segment_risk/"),
                      min_probability=a.min_probability, velocity_k=a.velocity_k, hubs=hubs)
    fx = {}
    for line in read_serving_csv(s3, a.bucket, f"{base}/fx_rates/").splitlines()[1:]:
        cur, rate = line.rsplit(",", 1)
        fx[cur.strip('"')] = float(rate)
    print(f"model loaded: {len(model.watch)} watchlist accounts, {len(model.hot)} hot segments, "
          f"{len(model.hubs)} hubs, velocity k={a.velocity_k}, {len(fx)} currencies")

    # consumer_timeout_ms ends the iteration when the stream goes quiet, so the last figures are saved too
    consumer = KafkaConsumer(a.topic, bootstrap_servers=a.bootstrap, group_id=a.group,
                             auto_offset_reset="earliest", enable_auto_commit=True, consumer_timeout_ms=5000,
                             value_deserializer=lambda b: json.loads(b.decode()))
    os.makedirs(OUT, exist_ok=True)
    local = open(os.path.join(OUT, "alerts.jsonl"), "a")
    buffer, latencies = [], []
    n_seen = n_alerts = n_true = 0
    last_flush = time.time()

    def flush():
        nonlocal buffer, last_flush
        local.flush()
        lat = latencies[-5000:]
        stats = {"events": n_seen, "alerts": n_alerts, "alert_rate": n_alerts / max(n_seen, 1),
                 "alert_precision": n_true / max(n_alerts, 1),
                 "latency_ms_p50": round(statistics.median(lat), 1) if lat else None,
                 "latency_ms_p95": round(sorted(lat)[int(0.95 * (len(lat) - 1))], 1) if lat else None,
                 "updated_at": time.time()}
        json.dump(stats, open(os.path.join(OUT, "stats.json"), "w"))
        if buffer:
            key = f"streaming/alerts/variant={a.variant}/alerts_{int(time.time())}_{os.getpid()}.json"
            s3.put_object(Bucket=a.bucket, Key=key, Body="\n".join(json.dumps(x) for x in buffer).encode())
            buffer = []
        print(stats, flush=True)
        last_flush = time.time()

    while True:
        for msg in consumer:
            ev = msg.value
            t = {"src_id": f"{ev['from_bank'].lstrip('0') or '0'}:{ev['from_account'].upper()}",
                 "dst_id": f"{ev['to_bank'].lstrip('0') or '0'}:{ev['to_account'].upper()}",
                 "from_bank": ev["from_bank"].lstrip("0"), "to_bank": ev["to_bank"].lstrip("0"),
                 "ts": ev["ts_raw"], "ts_epoch": time.mktime(time.strptime(ev["ts_raw"], "%Y/%m/%d %H:%M")),
                 "amount_usd": round(float(ev["amount_paid"]) / fx.get(ev["payment_currency"], 1.0), 2),
                 "payment_format": ev["payment_format"], "payment_currency": ev["payment_currency"],
                 "receiving_currency": ev["receiving_currency"], "is_laundering": int(ev["is_laundering"]),
                 "partition": msg.partition}
            n_seen += 1
            alert = model.score(t)
            latencies.append((time.time() - ev["produced_at"]) * 1000)
            if alert:
                n_alerts += 1
                n_true += alert["is_laundering"]  # label used ONLY to report alert precision on the dashboard
                alert["alerted_at"] = time.time()
                local.write(json.dumps(alert) + "\n")
                buffer.append(alert)
            if time.time() - last_flush >= a.flush_secs:
                flush()
        if n_seen and (buffer or time.time() - last_flush >= a.flush_secs):
            flush()


if __name__ == "__main__":
    main()
