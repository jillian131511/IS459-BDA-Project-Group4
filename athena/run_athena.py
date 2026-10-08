"""
Athena runner - executes the Q1 analytics (and, on request, the DDL or the CSV-vs-Parquet benchmark).

Runs as an AWS Glue *Python shell* job inside the Step Functions workflow (cheapest Glue option: 0.0625 DPU),
or from any laptop/EC2 with AWS credentials:

  python athena/run_athena.py --bucket BUCKET --variant HI-Small --mode q1
  python athena/run_athena.py --bucket BUCKET --mode ddl
  python athena/run_athena.py --bucket BUCKET --variant HI-Small --mode benchmark

Every query's DataScannedInBytes and engine time are logged to s3://BUCKET/analytics/run_log/ - that log is the
evidence behind our cost/performance claims (Athena charges per byte scanned - Week 5 slide 36).
"""
import argparse
import datetime as dt
import json
import os
import re
import sys
import time

import boto3

HERE = os.path.dirname(os.path.abspath(__file__))
Q1_FILES = ["q1_segment_risk", "q1_format_currency", "q1_time_profile", "q1_bank_corridors"]
PRICE_PER_TB = 5.0  # USD per TB scanned (Athena SQL, us-east-1 list price)


def parse_args():
    keys = ["bucket", "variant", "mode", "database", "workgroup", "region", "sql_prefix", "train_until"]
    if any(a.startswith("--BUCKET") for a in sys.argv):  # Glue Python shell passes --UPPER_CASE args
        from awsglue.utils import getResolvedOptions  # type: ignore
        g = getResolvedOptions(sys.argv, ["BUCKET", "VARIANT", "MODE", "TRAIN_UNTIL"])
        return argparse.Namespace(bucket=g["BUCKET"], variant=g["VARIANT"], mode=g["MODE"], database="aml",
                                  train_until=g["TRAIN_UNTIL"],
                                  workgroup="primary", region=os.environ.get("AWS_REGION", "us-east-1"),
                                  sql_prefix="scripts/athena")
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--variant", default="HI-Small")
    ap.add_argument("--mode", choices=["q1", "ddl", "benchmark"], default="q1")
    ap.add_argument("--database", default="aml")
    ap.add_argument("--workgroup", default="primary")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    ap.add_argument("--sql-prefix", dest="sql_prefix", default="scripts/athena")
    ap.add_argument("--train-until", dest="train_until", default="none",
                    help="YYYY-MM-DD: learn only from days before this date, so later days can be replayed "
                         "through the live monitor as genuinely unseen data")
    a = ap.parse_args()
    assert set(vars(a)) == set(keys)
    return a


class Athena:
    def __init__(self, a):
        self.a = a
        self.athena = boto3.client("athena", region_name=a.region)
        self.s3 = boto3.client("s3", region_name=a.region)
        self.log = []

    def read_sql(self, name):
        """Load SQL from the local folder, or from s3://bucket/<sql_prefix>/ when running on Glue."""
        local = os.path.join(HERE, name)
        if os.path.exists(local):
            return open(local).read()
        obj = self.s3.get_object(Bucket=self.a.bucket, Key=f"{self.a.sql_prefix}/{name}")
        return obj["Body"].read().decode()

    def run(self, sql, label, database=None):
        qid = self.athena.start_query_execution(
            QueryString=sql, WorkGroup=self.a.workgroup,
            QueryExecutionContext={"Database": database or self.a.database},
            ResultConfiguration={"OutputLocation": f"s3://{self.a.bucket}/athena-results/"})["QueryExecutionId"]
        while True:
            q = self.athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            state = q["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(2)
        stats = q.get("Statistics", {})
        scanned = stats.get("DataScannedInBytes", 0)
        entry = {"label": label, "query_id": qid, "state": state, "variant": self.a.variant,
                 "data_scanned_mb": round(scanned / 1e6, 2),
                 "engine_seconds": round(stats.get("EngineExecutionTimeInMillis", 0) / 1000, 2),
                 "cost_usd": round(max(scanned, 10e6) / 1e12 * PRICE_PER_TB, 6),  # 10 MB minimum per query
                 "at": dt.datetime.utcnow().isoformat(timespec="seconds")}
        self.log.append(entry)
        print(json.dumps(entry))
        if state != "SUCCEEDED":
            raise RuntimeError(f"{label}: {q['Status'].get('StateChangeReason', state)}")
        return qid

    def clear_prefix(self, prefix):
        """UNLOAD requires an empty target - delete the previous run's files for THIS variant only."""
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.a.bucket, Prefix=prefix):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                self.s3.delete_objects(Bucket=self.a.bucket, Delete={"Objects": keys})

    def save_log(self, kind):
        key = f"analytics/run_log/{kind}_{self.a.variant}_{dt.datetime.utcnow():%Y%m%dT%H%M%S}.json"
        self.s3.put_object(Bucket=self.a.bucket, Key=key, Body="\n".join(json.dumps(e) for e in self.log).encode())
        print("run log ->", f"s3://{self.a.bucket}/{key}")


def strip_comments(sql):
    return "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--")).strip().rstrip(";")


def run_q1(ath):
    a = ath.a
    for name in Q1_FILES:
        date_filter = "" if a.train_until.lower() == "none" else f"AND txn_date < '{a.train_until}'"
        body = (strip_comments(ath.read_sql(f"{name}.sql")).replace("{db}", a.database)
                .replace("{variant}", a.variant).replace("{date_filter}", date_filter))
        prefix = f"analytics/{name}/variant={a.variant}/"
        ath.clear_prefix(prefix)
        ath.run(f"UNLOAD ({body}) TO 's3://{a.bucket}/{prefix}' WITH (format = 'PARQUET', compression = 'SNAPPY')",
                f"q1:{name}")
    ath.save_log("q1")


def run_ddl(ath):
    ddl = ath.read_sql("ddl.sql").replace("{bucket}", ath.a.bucket)
    for stmt in [s for s in strip_comments(ddl).split(";") if s.strip()]:
        # the 'aml' database does not exist yet when it is being created -> run DDL from 'default'
        ath.run(stmt, "ddl:" + re.sub(r"\s+", " ", stmt)[:60], database="default")


def run_benchmark(ath):
    """Same business question, two storage layouts. Evidence for the 'columnar + partitioned' design."""
    a = ath.a
    v = a.variant
    qid = ath.run(f"SELECT MIN(txn_date) FROM aml.transactions WHERE variant = '{v}'", "benchmark:first_day")
    rows = ath.athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    day = rows[1]["Data"][0]["VarCharValue"]  # row 0 is the header
    qs = [
        ("full_scan_csv", f"SELECT payment_format, COUNT(*) n, SUM(is_laundering) l FROM aml.raw_transactions_csv "
                          f"WHERE variant = '{v}' GROUP BY payment_format"),
        ("full_scan_parquet", f"SELECT payment_format, COUNT(*) n, SUM(is_laundering) l FROM aml.transactions "
                              f"WHERE variant = '{v}' GROUP BY payment_format"),
        ("one_day_csv", f"SELECT payment_format, COUNT(*) n, SUM(is_laundering) l FROM aml.raw_transactions_csv "
                        f"WHERE variant = '{v}' AND ts_raw LIKE '{day.replace('-', '/')}%' GROUP BY payment_format"),
        ("one_day_parquet", f"SELECT payment_format, COUNT(*) n, SUM(is_laundering) l FROM aml.transactions "
                            f"WHERE variant = '{v}' AND txn_date = '{day}' GROUP BY payment_format"),
    ]
    for label, sql in qs:
        for rep in range(3):  # 3 repetitions -> report the median in the deck
            ath.run(sql, f"benchmark:{label}:{rep}")
    ath.save_log("benchmark")


def main():
    a = parse_args()
    ath = Athena(a)
    {"q1": run_q1, "ddl": run_ddl, "benchmark": run_benchmark}[a.mode](ath)


if __name__ == "__main__":
    main()
