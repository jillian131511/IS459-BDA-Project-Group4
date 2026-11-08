"""
Live AML monitor - scoring logic (pure Python, no Kafka dependency, unit-tested in tests/).

The batch layer (Glue + Athena + EMR) LEARNS where risk is; this streaming layer APPLIES it to every new
transaction within seconds, like a bank's real-time transaction-monitoring system:

  rule 1  WATCHLIST     sender or receiver is on the Q2 network watchlist (risk_probability >= threshold)
                        AND the transaction itself is in an above-average-risk segment (lift >= 2) - so a
                        watchlisted account's ordinary low-risk payments do not flood the queue
  rule 2  HOT SEGMENT   transaction falls in a Q1 segment with very high, statistically supported risk
  rule 3  VELOCITY      the sender has paid >= k distinct accounts in the last 24 h of event time
                        (stateful stream processing: a sliding window kept per account)

Ordering guarantee: the producer keys Kafka messages by sender account, so all of one account's transactions
land in the same partition and arrive in order - which rule 3's per-account state needs (Week 7 slide 24:
producers, topics, partitions, consumer groups).
"""
import csv
import io
from collections import defaultdict, deque

BANDS = [(100, "a <100"), (1_000, "b 100-1K"), (10_000, "c 1K-10K"), (100_000, "d 10K-100K"), (1_000_000, "e 100K-1M")]


def amount_band(usd):
    for lim, lab in BANDS:
        if usd < lim:
            return lab
    return "f >1M"


class RiskModel:
    def __init__(self, watchlist_csv_text, segments_csv_text, min_probability=0.5, min_segment_lift=20.0,
                 watchlist_segment_lift=2.0, velocity_k=11, velocity_window_secs=24 * 3600, hubs=()):
        self.watch = {}
        for r in csv.DictReader(io.StringIO(watchlist_csv_text)):
            p = float(r["risk_probability"])
            if p >= min_probability:
                self.watch[r["acct"]] = (p, r.get("reasons", ""))
        self.seg_lift, self.hot = {}, {}
        for r in csv.DictReader(io.StringIO(segments_csv_text)):
            key = (r["payment_format"], r["payment_currency"], int(float(r["is_cross_bank"])),
                   int(float(r["is_cross_currency"])), r["amount_band"])
            self.seg_lift[key] = float(r["lift"])
            # hot = very high lift AND statistically supported (Wilson lower bound above zero)
            if float(r["lift"]) >= min_segment_lift and float(r["wilson_lb"]) > 0:
                self.hot[key] = float(r["lift"])
        self.watchlist_segment_lift = watchlist_segment_lift
        self.velocity_k, self.window = velocity_k, velocity_window_secs
        # hub accounts (merchants, payroll: more counterparties than the batch layer's hub_degree) are left out of
        # the velocity rule, exactly as the batch layer leaves them out of its fan-out flags
        self.hubs = set(hubs)
        self.recent = defaultdict(deque)  # sender -> deque[(ts_epoch, receiver)]

    def score(self, t):
        """t: dict with src_id, dst_id, ts_epoch, amount_usd, payment_format, payment_currency,
        receiving_currency, from_bank, to_bank. Returns an alert dict or None."""
        reasons, severity = [], 0.0
        key = (t["payment_format"], t["payment_currency"], int(t["from_bank"] != t["to_bank"]),
               int(t["payment_currency"] != t["receiving_currency"]), amount_band(t["amount_usd"]))
        lift = self.seg_lift.get(key, 0.0)
        for role, acct in (("sender", t["src_id"]), ("receiver", t["dst_id"])):
            if acct in self.watch and lift >= self.watchlist_segment_lift:
                p, why = self.watch[acct]
                reasons.append(f"{role} on network watchlist (p={p:.2f}): {why.split(';')[0]}")
                severity = max(severity, p)
        if key in self.hot:
            reasons.append(f"high-risk segment {key[0]}/{key[1]}/{key[4]} (lift {self.hot[key]:.0f}x)")
            severity = max(severity, min(0.9, self.hot[key] / 100))
        distinct = 0
        if t["src_id"] not in self.hubs:
            dq = self.recent[t["src_id"]]
            dq.append((t["ts_epoch"], t["dst_id"]))
            while dq and dq[0][0] < t["ts_epoch"] - self.window:
                dq.popleft()
            distinct = len({d for _, d in dq})
        if distinct >= self.velocity_k:
            reasons.append(f"velocity: paid {distinct} distinct accounts in 24h (fan-out in progress)")
            severity = max(severity, 0.6)
        if not reasons:
            return None
        return {**t, "severity": round(severity, 3), "reasons": " | ".join(reasons)}
