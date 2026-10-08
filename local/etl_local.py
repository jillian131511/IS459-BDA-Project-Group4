"""
Local (pandas) twin of glue/etl_transactions.py, used ONLY for laptop testing without Spark.
It produces the same curated columns so every SQL file (Q1 + Q2) can be tested in SQLite.
Logic must stay in sync with the Glue job - see tests/test_pipeline.py.

Usage: python local/etl_local.py --trans data/raw/SYN-Small_Trans.csv --patterns data/raw/SYN-Small_Patterns.txt \
           --variant SYN-Small --out data/curated
"""
import argparse
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "glue"))
from patterns_parser import parse_patterns_text  # noqa: E402  (shared, pure-python)

RAW_COLS = ["ts_raw", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
            "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering"]
BANDS = [(100, "a <100"), (1_000, "b 100-1K"), (10_000, "c 1K-10K"), (100_000, "d 10K-100K"), (1_000_000, "e 100K-1M")]


def norm_bank(s):
    return s.astype(str).str.strip().str.replace(r"^0+(?=\d)", "", regex=True)


def fx_rates(df):
    a = df[(df.payment_currency == "US Dollar") & (df.receiving_currency != "US Dollar")]
    a = pd.DataFrame({"currency": a.receiving_currency, "rate": a.amount_received / a.amount_paid})
    b = df[(df.receiving_currency == "US Dollar") & (df.payment_currency != "US Dollar")]
    b = pd.DataFrame({"currency": b.payment_currency, "rate": b.amount_paid / b.amount_received})
    r = pd.concat([a, b]).groupby("currency").rate.agg(["median", "count"]).reset_index()
    r.columns = ["currency", "units_per_usd", "n_observations"]
    return pd.concat([r, pd.DataFrame([{"currency": "US Dollar", "units_per_usd": 1.0, "n_observations": 0}])])


def transform(raw, variant):
    df = raw.copy()
    n_in = len(df)
    df["from_bank"], df["to_bank"] = norm_bank(df.from_bank), norm_bank(df.to_bank)
    for c in ["from_account", "to_account"]:
        df[c] = df[c].astype(str).str.strip().str.upper()
    for c in ["payment_format", "payment_currency", "receiving_currency"]:
        df[c] = df[c].astype(str).str.strip()
    df["ts_parsed"] = pd.to_datetime(df.ts_raw.str.strip(), format="%Y/%m/%d %H:%M", errors="coerce")
    valid = df.ts_parsed.notna() & (df.amount_paid > 0) & df.is_laundering.isin([0, 1])
    df = df[valid]
    n_valid = len(df)
    df = df.drop_duplicates(subset=[c for c in df.columns if c != "ts_raw"])
    fx = fx_rates(df)
    df = df.merge(fx[["currency", "units_per_usd"]].rename(columns={"currency": "payment_currency"}),
                  on="payment_currency", how="left")
    df["amount_usd"] = (df.amount_paid / df.units_per_usd).round(2)
    t = df.ts_parsed
    df["ts_epoch"] = (t - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1)
    df["txn_date"] = t.dt.strftime("%Y-%m-%d")
    df["txn_hour"] = t.dt.hour
    df["day_of_week"] = t.dt.dayofweek + 1
    df["src_id"] = df.from_bank + ":" + df.from_account
    df["dst_id"] = df.to_bank + ":" + df.to_account
    df["is_self"] = (df.src_id == df.dst_id).astype(int)
    df["is_cross_bank"] = (df.from_bank != df.to_bank).astype(int)
    df["is_cross_currency"] = (df.payment_currency != df.receiving_currency).astype(int)
    df["is_round"] = ((df.amount_paid >= 1000) & (df.amount_paid % 1000 == 0)).astype(int)
    conds = [df.amount_usd < lim for lim, _ in BANDS]
    df["amount_band"] = np.select(conds, [lab for _, lab in BANDS], default="f >1M")
    df["txn_id"] = pd.util.hash_pandas_object(df[["ts_raw", "src_id", "dst_id", "amount_paid"]], index=False).astype("int64")
    df["variant"] = variant
    df["ts"] = t.dt.strftime("%Y-%m-%d %H:%M")
    cols = ["txn_id", "ts", "ts_epoch", "txn_hour", "day_of_week", "from_bank", "from_account", "to_bank",
            "to_account", "src_id", "dst_id", "amount_received", "receiving_currency", "amount_paid",
            "payment_currency", "amount_usd", "amount_band", "payment_format", "is_self", "is_cross_bank",
            "is_cross_currency", "is_round", "is_laundering", "variant", "txn_date"]
    out = df[cols]
    dq = {"variant": variant, "rows_in": n_in, "rows_valid": n_valid, "rows_malformed_dropped": n_in - n_valid,
          "rows_duplicate_dropped": n_valid - len(out), "rows_without_fx_rate": int(out.amount_usd.isna().sum()),
          "rows_out": len(out), "laundering_rows": int(out.is_laundering.sum())}
    return out, fx.assign(variant=variant), dq


def patterns(path, variant):
    rows = []
    with open(path) as f:
        for a, t, d, line in parse_patterns_text(f.read()):
            p = line.split(",")
            rows.append({"attempt_id": f"{variant}-{a}", "pattern_type": t, "pattern_detail": d,
                         "ts": pd.to_datetime(p[0], format="%Y/%m/%d %H:%M").strftime("%Y-%m-%d %H:%M"),
                         "src_id": f"{p[1].lstrip('0') or '0'}:{p[2].upper()}",
                         "dst_id": f"{p[3].lstrip('0') or '0'}:{p[4].upper()}",
                         "amount_paid": float(p[7]), "payment_currency": p[8], "payment_format": p[9],
                         "variant": variant})
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trans", required=True)
    ap.add_argument("--patterns")
    ap.add_argument("--variant", required=True)
    ap.add_argument("--out", default="data/curated")
    a = ap.parse_args()
    raw = pd.read_csv(a.trans, header=0, names=RAW_COLS, dtype={"from_bank": str, "to_bank": str,
                                                                 "from_account": str, "to_account": str})
    tx, fx, dq = transform(raw, a.variant)
    os.makedirs(a.out, exist_ok=True)
    tx.to_csv(os.path.join(a.out, "transactions.csv"), index=False)
    fx.to_csv(os.path.join(a.out, "fx_rates.csv"), index=False)
    pd.DataFrame([dq]).to_csv(os.path.join(a.out, "dq_report.csv"), index=False)
    if a.patterns:
        patterns(a.patterns, a.variant).to_csv(os.path.join(a.out, "patterns.csv"), index=False)
    print(dq)


if __name__ == "__main__":
    main()
