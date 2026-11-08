"""
Run the analytics layer (Q1 Athena SQL + Q2 network SQL) locally in SQLite on curated CSVs.
This executes the SAME SQL files that run on Athena (Q1) and EMR Spark SQL (Q2).

Usage: python local/run_local.py --curated data/curated --out data/analytics --variant SYN-Small
"""
import argparse
import glob
import math
import os
import sqlite3
import sys
import time

import pandas as pd

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "emr"))
from network_sql import DEFAULT_PARAMS, FUTURE_TRUTH_SQL, OUTPUTS, STEPS, render  # noqa: E402

Q1_FILES = ["q1_segment_risk", "q1_format_currency", "q1_time_profile", "q1_bank_corridors"]


def thresholds_from_stats(stats: pd.DataFrame):
    """Data-driven thresholds (same rule as emr/network_risk.py)."""
    return {
        "fan_out_k": max(5, int(math.ceil(stats.max_daily_fan_out.quantile(0.999)))),
        "fan_in_k": max(5, int(math.ceil(stats.max_daily_fan_in.quantile(0.999)))),
        "hub_degree": max(50, int(math.ceil(stats.total_degree.quantile(0.999)))),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--curated", default="data/curated")
    ap.add_argument("--out", default="data/analytics")
    ap.add_argument("--variant", default="SYN-Small")
    ap.add_argument("--train-until", default="", help="YYYY-MM-DD; learn only from earlier days")
    ap.add_argument("--test-until", default="", help="YYYY-MM-DD; official grading = train_until <= day < this")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    con = sqlite3.connect(":memory:")
    con.create_function("sqrt", 1, lambda x: None if x is None else math.sqrt(max(x, 0.0)))
    con.create_function("ln", 1, lambda x: None if x is None or x <= 0 else math.log(x))
    con.create_function("exp", 1, lambda x: None if x is None else math.exp(min(x, 700)))
    for name in ["transactions", "patterns", "fx_rates", "dq_report"]:
        p = os.path.join(a.curated, f"{name}.csv")
        if os.path.exists(p):
            df = pd.read_csv(p)
            if name == "transactions":
                df.to_sql("transactions_all", con, index=False)   # all days: source of future labels
                if a.train_until:
                    df = df[df.txn_date < a.train_until]
            df.to_sql(name, con, index=False)
    if a.train_until:
        con.execute("CREATE TEMP TABLE future_truth AS " + FUTURE_TRUTH_SQL.format(
            train_until=a.train_until, test_until=a.test_until or "9999-12-31"))
    else:
        con.execute("CREATE TEMP TABLE future_truth (acct TEXT, is_bad_official INT, is_bad_all INT)")
    con.execute("CREATE INDEX ix_t ON transactions(src_id)")

    t0 = time.time()
    for q in Q1_FILES:
        sql = open(os.path.join(ROOT, "athena", f"{q}.sql")).read().format(
            db="main", variant=a.variant, date_filter=f"AND txn_date < '{a.train_until}'" if a.train_until else "")
        df = pd.read_sql_query(sql, con)
        df.to_sql(q, con, index=False)
        df.to_csv(os.path.join(a.out, f"{q}.csv"), index=False)
        print(f"Q1 {q:22s} {len(df):6d} rows")
    print(f"Q1 done in {time.time() - t0:.1f}s")

    params = dict(DEFAULT_PARAMS)
    t0 = time.time()
    for name, sql in STEPS:
        if name == "dp_nh":
            params.update(thresholds_from_stats(pd.read_sql_query("SELECT * FROM account_stats", con)))
            print("thresholds:", {k: params[k] for k in ("fan_out_k", "fan_in_k", "hub_degree")})
        s = time.time()
        con.execute(f"CREATE TEMP TABLE {name} AS {render(sql, params)}")
        n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"Q2 {name:18s} {n:8d} rows  {time.time() - s:5.1f}s")
        if name in ("pairs", "account_stats"):
            con.execute(f"CREATE INDEX ix_{name} ON {name}({'src_id, dst_id' if name == 'pairs' else 'acct'})")
        if name == "dp_nh":
            con.execute("CREATE INDEX ix_dp_sd ON dp_nh(src_id, dst_id)")
        if name == "dp_shift":
            con.execute("CREATE INDEX ix_dps ON dp_shift(src_id, join_day)")
        if name == "paths2":
            con.execute("CREATE INDEX ix_p2 ON paths2(a, c)")
    print(f"Q2 done in {time.time() - t0:.1f}s")
    for name in OUTPUTS:
        pd.read_sql_query(f"SELECT * FROM {name}", con).to_csv(os.path.join(a.out, f"q2_{name}.csv"), index=False)
    # hub accounts (more counterparties than hub_degree on the learning days), as emr/network_risk.py publishes
    # them: the live velocity rule skips them
    pd.read_sql_query(f"SELECT acct FROM account_stats WHERE total_degree > {params['hub_degree']}", con)         .to_csv(os.path.join(a.out, "hubs.csv"), index=False)
    for f in ["dq_report", "fx_rates"]:
        pd.read_sql_query(f"SELECT * FROM {f}", con).to_csv(os.path.join(a.out, f"{f}.csv"), index=False)
    print("outputs:", sorted(os.path.basename(p) for p in glob.glob(os.path.join(a.out, "*.csv"))))


if __name__ == "__main__":
    main()
