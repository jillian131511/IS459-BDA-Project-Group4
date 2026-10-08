"""
Generate a small synthetic dataset that mimics the IBM AML (AMLworld) Kaggle files EXACTLY in format:

  <variant>_Trans.csv     Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,
                          Amount Paid,Payment Currency,Payment Format,Is Laundering
  <variant>_Patterns.txt  BEGIN LAUNDERING ATTEMPT - <TYPE>: <detail>
                          <transaction rows in the same CSV layout>
                          END LAUNDERING ATTEMPT - <TYPE>

Purpose: let the team (and CI) test every pipeline stage before/without downloading the real 16 GB data.
The numbers it produces are NOT real findings - never put synthetic results in the final presentation.

Usage: python local/make_synthetic_data.py --out data/raw --variant SYN-Small --accounts 20000 --txns 300000
"""
import argparse
import os
import random
from datetime import datetime, timedelta

FORMATS = ["ACH", "Cheque", "Credit Card", "Wire", "Cash", "Reinvestment", "Bitcoin"]
FORMAT_WEIGHTS = [0.25, 0.20, 0.25, 0.10, 0.10, 0.06, 0.04]
# units of currency per 1 USD (synthetic, roughly realistic)
CURRENCIES = {
    "US Dollar": 1.0, "Euro": 0.99, "UK Pound": 0.86, "Yuan": 6.9, "Yen": 140.0,
    "Rupee": 79.5, "Canadian Dollar": 1.31, "Australian Dollar": 1.47, "Swiss Franc": 0.97,
    "Mexican Peso": 20.0, "Brazil Real": 5.2, "Ruble": 60.0, "Shekel": 3.4, "Saudi Riyal": 3.75,
    "Bitcoin": 0.00005,
}
CUR_WEIGHTS = [0.40, 0.15, 0.06, 0.06, 0.05, 0.05, 0.04, 0.04, 0.03, 0.03, 0.03, 0.02, 0.02, 0.01, 0.01]
START = datetime(2022, 9, 1)


def hex_acct(rng):
    return "8" + "".join(rng.choice("0123456789ABCDEF") for _ in range(8))


def fmt_row(ts, fb, fa, tb, ta, usd, rcur, pcur, pfmt, lab):
    paid = usd * CURRENCIES[pcur]
    recv = usd * CURRENCIES[rcur]
    rd = 6 if rcur == "Bitcoin" else 2
    pd_ = 6 if pcur == "Bitcoin" else 2
    return (f"{ts:%Y/%m/%d %H:%M},{fb},{fa},{tb},{ta},{recv:.{rd}f},{rcur},{paid:.{pd_}f},{pcur},{pfmt},{lab}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--variant", default="SYN-Small")
    ap.add_argument("--accounts", type=int, default=20000)
    ap.add_argument("--txns", type=int, default=300000)
    ap.add_argument("--days", type=int, default=10)
    ap.add_argument("--attempts", type=int, default=120)
    ap.add_argument("--seed", type=int, default=459)
    a = ap.parse_args()
    rng = random.Random(a.seed)

    banks = [str(rng.randint(10, 300000)) for _ in range(40)]
    accts = [(rng.choice(banks), hex_acct(rng)) for _ in range(a.accounts)]
    home_cur = {acc: rng.choices(list(CURRENCIES), CUR_WEIGHTS)[0] for acc in accts}
    hubs = rng.sample(accts, 15)  # merchants/payroll: legit high-degree accounts (tests hub pruning)
    span = a.days * 24 * 60

    rows = []  # (ts, line)
    for _ in range(a.txns):
        ts = START + timedelta(minutes=rng.randrange(span))
        src = rng.choice(accts)
        r = rng.random()
        if r < 0.10:
            dst = src  # self transfer / reinvestment
            pfmt = "Reinvestment"
        else:
            dst = rng.choice(hubs) if r < 0.25 else rng.choice(accts)
            pfmt = rng.choices(FORMATS, FORMAT_WEIGHTS)[0]
        usd = round(rng.lognormvariate(6.0, 1.6), 2)
        pcur = home_cur[src]
        rcur = home_cur[dst] if (pfmt in ("Wire", "ACH") and rng.random() < 0.3) else pcur
        if pfmt == "Bitcoin":
            pcur = rcur = "Bitcoin"
        rows.append((ts, fmt_row(ts, src[0], src[1], dst[0], dst[1], usd, rcur, pcur, pfmt, 0)))

    patterns = []
    types = ["FAN-OUT", "FAN-IN", "CYCLE", "SCATTER-GATHER", "GATHER-SCATTER", "STACK", "RANDOM", "BIPARTITE"]
    laund_formats = ["ACH", "ACH", "ACH", "Wire", "Cash", "Bitcoin", "Cheque"]  # skewed: gives Q1 a signal
    for i in range(a.attempts):
        t = types[i % len(types)]
        t0 = START + timedelta(minutes=rng.randrange(span - 3 * 24 * 60))
        usd = round(rng.uniform(2000, 40000), 2)
        pf = rng.choice(laund_formats)
        edges = []
        if t == "FAN-OUT":
            s = rng.choice(accts); k = rng.randint(5, 16)
            edges = [(s, d) for d in rng.sample(accts, k)]
            detail = f"Max {k}-degree Fan-Out"
        elif t == "FAN-IN":
            d = rng.choice(accts); k = rng.randint(5, 16)
            edges = [(s, d) for s in rng.sample(accts, k)]
            detail = f"Max {k}-degree Fan-In"
        elif t == "CYCLE":
            k = rng.randint(2, 6); nodes = rng.sample(accts, k)
            edges = [(nodes[j], nodes[(j + 1) % k]) for j in range(k)]
            detail = f"Max {k} hops"
        elif t == "SCATTER-GATHER":
            s, d = rng.sample(accts, 2); mids = rng.sample(accts, rng.randint(3, 8))
            edges = [(s, m) for m in mids] + [(m, d) for m in mids]
            detail = f"{len(mids)} intermediaries"
        elif t == "GATHER-SCATTER":
            hub = rng.choice(accts)
            edges = [(s, hub) for s in rng.sample(accts, rng.randint(4, 8))] + \
                    [(hub, d) for d in rng.sample(accts, rng.randint(4, 8))]
            detail = "gather then scatter"
        elif t == "STACK":
            l1, l2, l3 = (rng.sample(accts, 3) for _ in range(3))
            edges = [(x, y) for x in l1 for y in l2] + [(y, z) for y in l2 for z in l3]
            detail = "3 layers"
        elif t == "BIPARTITE":
            l1, l2 = rng.sample(accts, 3), rng.sample(accts, 3)
            edges = [(x, y) for x in l1 for y in l2]
            detail = "3x3"
        else:
            nodes = rng.sample(accts, rng.randint(3, 6))
            edges = [(nodes[j], nodes[j + 1]) for j in range(len(nodes) - 1)]
            detail = f"{len(nodes)} nodes"
        lines = []
        for j, (s, d) in enumerate(edges):
            ts = t0 + timedelta(minutes=j * rng.randint(5, 180))
            amt = round(usd * rng.uniform(0.85, 1.0), 2)
            pcur = home_cur[s] if pf != "Bitcoin" else "Bitcoin"
            line = fmt_row(ts, s[0], s[1], d[0], d[1], amt, pcur, pcur, pf, 1)
            rows.append((ts, line)); lines.append((ts, line))
        lines.sort()
        patterns.append(f"BEGIN LAUNDERING ATTEMPT - {t}: {detail}")
        patterns += [l for _, l in lines]
        patterns.append(f"END LAUNDERING ATTEMPT - {t}")
        patterns.append("")

    rows.sort(key=lambda x: x[0])
    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, f"{a.variant}_Trans.csv"), "w") as f:
        # NOTE: the real file repeats the column name "Account" - we reproduce that quirk on purpose.
        f.write("Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,"
                "Amount Paid,Payment Currency,Payment Format,Is Laundering\n")
        f.write("\n".join(l for _, l in rows) + "\n")
    with open(os.path.join(a.out, f"{a.variant}_Patterns.txt"), "w") as f:
        f.write("\n".join(patterns))
    print(f"wrote {len(rows):,} transactions and {a.attempts} laundering attempts to {a.out}")


if __name__ == "__main__":
    main()
