"""
Kafka PRODUCER - replays IBM AML transactions as a live event stream.

Reads the raw <variant>_Trans.csv (from S3 or a local path) in timestamp order and publishes each row as a JSON
event to topic `aml-transactions`, keyed by sender account (=> per-account ordering within a partition).
--speed 3600 means one hour of dataset time is replayed per second of wall-clock time.

  python streaming/producer.py --source s3://BUCKET/raw/transactions/variant=HI-Small/HI-Small_Trans.csv \
      --bootstrap localhost:9092 --start "2022/09/09" --max-events 200000 --speed 3600
"""
import argparse
from contextlib import closing
import csv
import io
import json
import os
import sqlite3
import tempfile
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


def ordered_events(source, start="", end="", limit=100000):
    """Sort the selected interval on disk; limit applies AFTER chronological ordering.

    A temporary SQLite database keeps large replay intervals out of Python memory.
    Equal timestamps retain their original file order. No future events are discarded.
    """
    with tempfile.TemporaryDirectory(prefix="aml-replay-") as tmp:
        with closing(sqlite3.connect(os.path.join(tmp, "events.sqlite"))) as db:
            db.execute("PRAGMA temp_store=FILE")
            db.execute("CREATE TABLE events (seq INTEGER PRIMARY KEY, ts TEXT, payload TEXT)")
            with open_source(source) as stream:
                reader = csv.reader(stream)
                next(reader, None)

                def selected_rows():
                    for seq, row in enumerate(reader):
                        if len(row) != len(COLS) or row[0] < start or (end and row[0] >= end):
                            continue
                        # Validate before publishing anything; lexical order matches this fixed-width format.
                        stamp = time.strptime(row[0], "%Y/%m/%d %H:%M")
                        if time.strftime("%Y/%m/%d %H:%M", stamp) != row[0]:
                            raise ValueError(f"Non-canonical timestamp: {row[0]}")
                        yield seq, row[0], json.dumps(dict(zip(COLS, row)))

                db.executemany("INSERT INTO events VALUES (?, ?, ?)", selected_rows())
            db.commit()
            for (payload,) in db.execute("SELECT payload FROM events ORDER BY ts, seq LIMIT ?", (limit,)):
                yield json.loads(payload)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="aml-transactions")
    ap.add_argument("--start", default="", help="skip rows before this timestamp prefix, e.g. 2022/09/09")
    ap.add_argument("--end", default="", help="skip rows from this timestamp prefix on, e.g. 2022/09/11")
    ap.add_argument("--max-events", type=int, default=100000)
    ap.add_argument("--speed", type=float, default=3600.0)
    a = ap.parse_args()
    if a.speed <= 0 or a.max_events <= 0:
        ap.error("--speed and --max-events must be positive")

    producer = KafkaProducer(bootstrap_servers=a.bootstrap, linger_ms=20, acks=1,
                             max_in_flight_requests_per_connection=1,
                             key_serializer=str.encode,
                             value_serializer=lambda v: json.dumps(v).encode())
    sent, first_event_ts, t0 = 0, None, None
    print("Preparing timestamp-ordered replay on temporary disk (reads the source before sending)...", flush=True)
    for ev in ordered_events(a.source, a.start, a.end, a.max_events):
        event_ts = time.mktime(time.strptime(ev["ts_raw"], "%Y/%m/%d %H:%M"))
        if first_event_ts is None:
            first_event_ts = event_ts
            t0 = time.monotonic()
        # pace the replay: wait until this event's (scaled) time has come
        due = t0 + (event_ts - first_event_ts) / a.speed
        delay = due - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        ev["produced_at"] = time.time()
        sender = f"{ev['from_bank'].lstrip('0') or '0'}:{ev['from_account'].upper()}"
        producer.send(a.topic, key=sender, value=ev)
        sent += 1
        if sent % 10000 == 0:
            print(f"sent {sent:,} events (dataset time {ev['ts_raw']})")
    producer.flush()
    producer.close()
    elapsed = time.monotonic() - t0 if t0 is not None else 0
    print(f"done: {sent:,} events in {elapsed:.0f}s (timestamp ordered; no out-of-order rows dropped)")


if __name__ == "__main__":
    main()
