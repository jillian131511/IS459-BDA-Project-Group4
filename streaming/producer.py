"""
Kafka PRODUCER - replays IBM AML transactions as a live event stream.

Reads the raw <variant>_Trans.csv (from S3 or a local path) in timestamp order and publishes each row as a JSON
event to topic `aml-transactions`, keyed by sender account (=> per-account ordering within a partition).
--speed 3600 means one hour of dataset time is replayed per second of wall-clock time.

  python streaming/producer.py --source s3://BUCKET/raw/transactions/variant=HI-Small/HI-Small_Trans.csv \
      --bootstrap localhost:9092 --start "2022/09/09" --max-events 200000 --speed 3600
"""
import argparse
import csv
import io
import json
import time

import boto3
from kafka import KafkaProducer

COLS = ["ts_raw", "from_bank", "from_account", "to_bank", "to_account", "amount_received", "receiving_currency",
        "amount_paid", "payment_currency", "payment_format", "is_laundering"]


def open_source(src):
    if src.startswith("s3://"):
        bucket, key = src[5:].split("/", 1)
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"]
        return io.TextIOWrapper(body, encoding="utf-8")
    return open(src, encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="aml-transactions")
    ap.add_argument("--start", default="", help="skip rows before this timestamp prefix, e.g. 2022/09/09")
    ap.add_argument("--end", default="", help="skip rows from this timestamp prefix on, e.g. 2022/09/11")
    ap.add_argument("--max-ahead-mins", type=float, default=60,
                    help="skip rows more than this far ahead of the stream's clock: a few later-dated rows (mostly "
                         "laundering) sit early in the raw file and would make the replay unrepresentative")
    ap.add_argument("--max-wait", type=float, default=0.5,
                    help="longest pause between two events (s); the raw file is only roughly in time order")
    ap.add_argument("--max-events", type=int, default=100000)
    ap.add_argument("--speed", type=float, default=3600.0)
    a = ap.parse_args()

    producer = KafkaProducer(bootstrap_servers=a.bootstrap, linger_ms=20, acks=1,
                             key_serializer=str.encode,
                             value_serializer=lambda v: json.dumps(v).encode())
    reader = csv.reader(open_source(a.source))
    next(reader)  # header
    sent, skipped_ahead, first_event_ts, t0 = 0, 0, None, time.time()
    clock = time.mktime(time.strptime(a.start, "%Y/%m/%d")) if a.start else None
    for row in reader:
        if len(row) < 11 or row[0] < a.start or (a.end and row[0] >= a.end):
            continue
        ev = dict(zip(COLS, row))
        event_ts = time.mktime(time.strptime(ev["ts_raw"], "%Y/%m/%d %H:%M"))
        if clock is not None and event_ts > clock + a.max_ahead_mins * 60:
            skipped_ahead += 1
            continue
        clock = event_ts if clock is None else max(clock, event_ts)
        if first_event_ts is None:
            first_event_ts = event_ts
        # pace the replay: wait until this event's (scaled) time has come
        # (a few later-dated rows sit early in the file, so one pause is capped instead of waiting for "hours")
        due = t0 + (event_ts - first_event_ts) / a.speed
        if due > time.time():
            time.sleep(min(due - time.time(), a.max_wait))
        ev["produced_at"] = time.time()
        producer.send(a.topic, key=f"{ev['from_bank']}:{ev['from_account']}", value=ev)
        sent += 1
        if sent % 10000 == 0:
            print(f"sent {sent:,} events (dataset time {ev['ts_raw']})")
        if sent >= a.max_events:
            break
    producer.flush()
    print(f"done: {sent:,} events in {time.time() - t0:.0f}s ({skipped_ahead:,} out-of-order rows skipped)")


if __name__ == "__main__":
    main()
