"""
Local stand-in for producer + Kafka + consumer: replays the last N days of the synthetic raw CSV through
streaming/monitor_core.py exactly as the Kafka consumer would, and writes streaming/out/alerts.jsonl + stats.json
so the dashboard's Live monitor tab can be developed and tested without Kafka.

Usage: python local/replay_local.py --raw data/raw/SYN-Small_Trans.csv --analytics data/analytics --curated data/curated
"""
import argparse
import csv
import json
import os
import statistics
import sys
import time

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "streaming"))
from monitor_core import RiskModel  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="data/raw/SYN-Small_Trans.csv")
    ap.add_argument("--analytics", default="data/analytics")
    ap.add_argument("--curated", default="data/curated")
    ap.add_argument("--start", default="2022/09/08")
    ap.add_argument("--out", default=os.path.join(ROOT, "streaming", "out"))
    # same settings as the Kafka consumer (streaming/consumer.py): threshold 11, hub accounts excluded
    ap.add_argument("--velocity-k", type=int, default=11, help="distinct payees in 24 h that raise a velocity alert")
    a = ap.parse_args()

    hubs_csv = os.path.join(a.analytics, "hubs.csv")
    hubs = [r["acct"] for r in csv.DictReader(open(hubs_csv))] if os.path.exists(hubs_csv) else []
    model = RiskModel(open(os.path.join(a.analytics, "q2_watchlist.csv")).read(),
                      open(os.path.join(a.analytics, "q1_segment_risk.csv")).read(),
                      velocity_k=a.velocity_k, hubs=hubs)
    print(f"velocity k={a.velocity_k}, {len(hubs)} hubs excluded"
          + ("" if hubs else " (no hubs.csv: rerun local/run_local.py)"))
    fx = {r["currency"]: float(r["units_per_usd"]) for r in csv.DictReader(open(os.path.join(a.curated, "fx_rates.csv")))}
    os.makedirs(a.out, exist_ok=True)
    alerts, lat, n = [], [], 0
    rd = csv.reader(open(a.raw))
    next(rd)
    for row in rd:
        if row[0] < a.start:
            continue
        t0 = time.perf_counter()
        t = {"src_id": f"{row[1].lstrip('0') or '0'}:{row[2].upper()}", "dst_id": f"{row[3].lstrip('0') or '0'}:{row[4].upper()}",
             "from_bank": row[1].lstrip("0"), "to_bank": row[3].lstrip("0"), "ts": row[0],
             "ts_epoch": time.mktime(time.strptime(row[0], "%Y/%m/%d %H:%M")),
             "amount_usd": round(float(row[7]) / fx.get(row[8], 1.0), 2), "payment_format": row[9],
             "payment_currency": row[8], "receiving_currency": row[6], "is_laundering": int(row[10]),
             "partition": hash(row[1] + row[2]) % 3}
        n += 1
        al = model.score(t)
        lat.append((time.perf_counter() - t0) * 1000)
        if al:
            al["alerted_at"] = time.time()
            alerts.append(al)
    with open(os.path.join(a.out, "alerts.jsonl"), "w") as f:
        f.write("\n".join(json.dumps(x) for x in alerts) + "\n")
    tp = sum(x["is_laundering"] for x in alerts)
    tot_l = None
    stats = {"events": n, "alerts": len(alerts), "alert_rate": len(alerts) / max(n, 1),
             "alert_precision": tp / max(len(alerts), 1),
             "latency_ms_p50": round(statistics.median(lat), 3), "latency_ms_p95": round(sorted(lat)[int(0.95 * (len(lat) - 1))], 3),
             "updated_at": time.time(), "mode": "local replay (no Kafka)"}
    json.dump(stats, open(os.path.join(a.out, "stats.json"), "w"))
    print(stats, tot_l)


if __name__ == "__main__":
    main()
