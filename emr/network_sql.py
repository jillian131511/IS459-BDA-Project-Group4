"""
Q2 - Laundering-network detection, written as an ordered list of portable SQL steps.

The SAME SQL runs in two engines:
  * Spark SQL on Amazon EMR (emr/network_risk.py)  - production, HI-Small .. HI-Large
  * SQLite on a laptop (local/run_local.py)        - fast tests on synthetic data
so the logic we test locally is exactly the logic that runs on the cluster.

Each step becomes a temp view (Spark) / temp table (SQLite) named after the step.
Inputs expected: transactions (one variant, curated schema), q1_segment_risk (Athena Q1 output), patterns.
Placeholders: {window_days} {pass_lo} {pass_hi} {hub_degree} {fan_out_k} {fan_in_k} {gs_k} {sg_k} {top_k}.

Graph vocabulary used (matches the AMLworld laundering typologies - Altman et al., NeurIPS 2023):
  fan-out / fan-in   one account sends to / receives from unusually many accounts in a day
  cycle (2-4 hops)   money returns to where it started, within the time window
  scatter-gather     A splits money across >= k "mule" accounts that all forward it to the same C
  gather-scatter     one account collects from many and pays out to many on the same day
"""

STEPS = [
    # 1. Directed edges, one per transaction (self-transfers carry no network signal)
    ("edges", """
SELECT src_id, dst_id, ts_epoch, txn_date, amount_usd, payment_format, payment_currency,
       is_cross_bank, is_cross_currency, amount_band, is_laundering
FROM transactions
WHERE is_self = 0
"""),
    # 2. Collapse repeated transfers between the same two accounts (keeps first/last time for temporal joins)
    ("pairs", """
SELECT src_id, dst_id, MIN(ts_epoch) AS first_ts, MAX(ts_epoch) AS last_ts,
       COUNT(*) AS n_txn, SUM(amount_usd) AS usd, MAX(is_laundering) AS any_laundering
FROM edges
GROUP BY src_id, dst_id
"""),
    # 3. Distinct counterparties per account per day
    ("daily_degree", """
SELECT acct, txn_date, SUM(out_deg) AS out_deg, SUM(in_deg) AS in_deg
FROM (
    SELECT src_id AS acct, txn_date, COUNT(DISTINCT dst_id) AS out_deg, 0 AS in_deg
    FROM edges GROUP BY src_id, txn_date
    UNION ALL
    SELECT dst_id AS acct, txn_date, 0 AS out_deg, COUNT(DISTINCT src_id) AS in_deg
    FROM edges GROUP BY dst_id, txn_date
) x
GROUP BY acct, txn_date
"""),
    # 4. Per-account degree statistics
    ("account_stats", """
SELECT d.acct,
       MAX(d.out_deg) AS max_daily_fan_out,
       MAX(d.in_deg)  AS max_daily_fan_in,
       MAX(CASE WHEN d.out_deg >= {gs_k} AND d.in_deg >= {gs_k} THEN 1 ELSE 0 END) AS gather_scatter_day,
       MAX(t.total_degree) AS total_degree
FROM daily_degree d
JOIN (
    SELECT acct, COUNT(DISTINCT nbr) AS total_degree
    FROM (SELECT src_id AS acct, dst_id AS nbr FROM pairs
          UNION ALL
          SELECT dst_id AS acct, src_id AS nbr FROM pairs) u
    GROUP BY acct
) t ON t.acct = d.acct
GROUP BY d.acct
"""),
    # 5. Daily account-to-account flows between NON-hub accounts. Hubs (payroll, merchants) would explode the
    #    path joins and are not where layering happens; they still receive fan-in/fan-out statistics.
    ("dp_nh", """
SELECT e.src_id, e.dst_id, CAST(e.ts_epoch / 86400 AS INT) AS day,
       SUM(e.amount_usd) AS usd, COUNT(*) AS n_txn
FROM edges e
JOIN account_stats s1 ON s1.acct = e.src_id AND s1.total_degree <= {hub_degree}
JOIN account_stats s2 ON s2.acct = e.dst_id AND s2.total_degree <= {hub_degree}
GROUP BY e.src_id, e.dst_id, CAST(e.ts_epoch / 86400 AS INT)
"""),
    # 6. Each second-hop flow is copied once per day offset 0..window_days, so the time window becomes an
    #    EQUI-join on (account, day). Work then grows with an account's DAILY activity, not its lifetime
    #    activity - the difference between minutes and hours on 180M rows.
    ("dp_shift", """
SELECT d.src_id, d.dst_id, d.day, d.usd, d.day - o.o AS join_day
FROM dp_nh d
CROSS JOIN ({offsets_sql}) o
"""),
    # 7. Time-respecting two-hop paths a -> b -> c where b forwards a similar amount within the window
    #    ("pass-through": layering moves most of the money on, minus fees).
    ("paths2", """
SELECT p1.src_id AS a, p1.dst_id AS b, p2.dst_id AS c, p1.day AS d1, p2.day AS d2,
       p1.usd AS usd1, p2.usd AS usd2
FROM dp_nh p1
JOIN dp_shift p2 ON p2.src_id = p1.dst_id AND p2.join_day = p1.day
WHERE p2.usd >= {pass_lo} * p1.usd AND p2.usd <= {pass_hi} * p1.usd
"""),
    # 8. Cycles of length 2, 3 and 4 that close within the window, one row per (start account, cycle).
    #    Each rotation of a cycle starts at a different member, so every member is counted once per cycle.
    ("cycles", """
SELECT DISTINCT a AS acct, b AS k1, a AS k2, 2 AS cycle_len FROM paths2 WHERE c = a
UNION
SELECT DISTINCT p.a AS acct, p.b AS k1, p.c AS k2, 3 AS cycle_len
FROM paths2 p
JOIN dp_nh p3 ON p3.src_id = p.c AND p3.dst_id = p.a
WHERE p.c <> p.a
  AND p3.day >= p.d2
  AND p3.day <= p.d1 + 2 * {window_days}
UNION
SELECT DISTINCT x.a AS acct, x.b AS k1, x.c AS k2, 4 AS cycle_len
FROM paths2 x
JOIN paths2 y ON y.a = x.c AND y.c = x.a
WHERE x.c <> x.a AND x.b <> y.b
  AND y.b <> x.a AND y.b <> x.c AND x.b <> x.c
  AND y.d1 >= x.d2
  AND y.d2 <= x.d1 + 3 * {window_days}
"""),
    # 9. Scatter-gather: a reaches c through >= k distinct intermediaries
    ("scatter_gather", """
SELECT a, c, COUNT(DISTINCT b) AS n_mules, SUM(usd1) AS usd_out
FROM paths2
WHERE a <> c
GROUP BY a, c
HAVING COUNT(DISTINCT b) >= {sg_k}
"""),
    ("sg_roles", """
SELECT acct, SUM(is_source) AS sg_as_source, SUM(is_sink) AS sg_as_sink, SUM(is_mule) AS sg_as_mule
FROM (
    SELECT a AS acct, 1 AS is_source, 0 AS is_sink, 0 AS is_mule FROM scatter_gather
    UNION ALL
    SELECT c AS acct, 0 AS is_source, 1 AS is_sink, 0 AS is_mule FROM scatter_gather
    UNION ALL
    SELECT DISTINCT p.b AS acct, 0 AS is_source, 0 AS is_sink, 1 AS is_mule
    FROM paths2 p JOIN scatter_gather s ON s.a = p.a AND s.c = p.c
) r
GROUP BY acct
"""),
    # 10. Link Q1 into Q2: how risky are the SEGMENTS an account transacts in? (avg Q1 lift of its transactions)
    ("segment_exposure", """
SELECT acct, AVG(lift) AS seg_lift_avg, MAX(lift) AS seg_lift_max
FROM (
    SELECT e.src_id AS acct, q.lift FROM edges e
    JOIN q1_segment_risk q
      ON q.payment_format = e.payment_format AND q.payment_currency = e.payment_currency
     AND q.is_cross_bank = e.is_cross_bank AND q.is_cross_currency = e.is_cross_currency
     AND q.amount_band = e.amount_band
    UNION ALL
    SELECT e.dst_id AS acct, q.lift FROM edges e
    JOIN q1_segment_risk q
      ON q.payment_format = e.payment_format AND q.payment_currency = e.payment_currency
     AND q.is_cross_bank = e.is_cross_bank AND q.is_cross_currency = e.is_cross_currency
     AND q.amount_band = e.amount_band
) z
GROUP BY acct
"""),
    # 11. Ground truth (used ONLY for evaluation, never for scoring): touched any laundering transaction?
    ("truth", """
SELECT acct, MAX(is_laundering) AS is_bad
FROM (SELECT src_id AS acct, is_laundering FROM edges
      UNION ALL
      SELECT dst_id AS acct, is_laundering FROM edges) u
GROUP BY acct
"""),
    # 12. One row per account with its red-flag features. Accounts are split 50/50 by a hex digit of
    #     the account id: CALIBRATION accounts are used to learn how much each flag is worth, HOLDOUT
    #     accounts are used only to measure performance -> no label leakage in the reported numbers.
    ("account_features", """
SELECT s.acct, s.max_daily_fan_out, s.max_daily_fan_in, s.gather_scatter_day, s.total_degree,
       COALESCE(c.n_cycles, 0) AS n_cycles, COALESCE(c.max_cycle_len, 0) AS max_cycle_len,
       COALESCE(g.sg_as_source, 0) AS sg_as_source, COALESCE(g.sg_as_sink, 0) AS sg_as_sink,
       COALESCE(g.sg_as_mule, 0) AS sg_as_mule,
       COALESCE(x.seg_lift_avg, 0) AS seg_lift_avg,
       CASE WHEN s.total_degree > {hub_degree} THEN 1 ELSE 0 END AS is_hub,
       CASE WHEN s.max_daily_fan_out >= {fan_out_k} AND s.total_degree <= {hub_degree} THEN 1 ELSE 0 END AS f_fan_out,
       CASE WHEN s.max_daily_fan_in  >= {fan_in_k}  AND s.total_degree <= {hub_degree} THEN 1 ELSE 0 END AS f_fan_in,
       CASE WHEN COALESCE(c.n_cycles, 0) > 0 THEN 1 ELSE 0 END AS f_cycle,
       CASE WHEN COALESCE(g.sg_as_source, 0) + COALESCE(g.sg_as_sink, 0) > 0 THEN 1 ELSE 0 END AS f_sg_end,
       CASE WHEN COALESCE(g.sg_as_mule, 0) > 0 THEN 1 ELSE 0 END AS f_sg_mule,
       s.gather_scatter_day AS f_gather_scatter,
       CASE WHEN COALESCE(x.seg_lift_avg, 0) >= 10 THEN 3
            WHEN COALESCE(x.seg_lift_avg, 0) >= 3 THEN 2
            WHEN COALESCE(x.seg_lift_avg, 0) >= 1 THEN 1 ELSE 0 END AS f_segment,
       -- 2nd-to-last character: evenly spread over 0-F. (The LAST character is '0' for ~97% of IBM AML account
       -- ids, which made the "holdout half" only 3% of accounts - found on the first HI-Small run.)
       CASE WHEN substr(s.acct, -2, 1) IN ('0','2','4','6','8','A','C','E') THEN 'calibration' ELSE 'holdout' END AS split,
       t.is_bad
FROM account_stats s
JOIN truth t ON t.acct = s.acct
LEFT JOIN (SELECT acct, COUNT(*) AS n_cycles, MAX(cycle_len) AS max_cycle_len FROM cycles GROUP BY acct) c
       ON c.acct = s.acct
LEFT JOIN sg_roles g ON g.acct = s.acct
LEFT JOIN segment_exposure x ON x.acct = s.acct
"""),
    # 13. Weight of Evidence (classic credit-risk scorecard technique) learned on CALIBRATION accounts only:
    #     woe = ln( P(flag value | laundering account) / P(flag value | normal account) ), with +0.5 smoothing.
    #     Positive woe = evidence of laundering. Score = prior log-odds + sum of woe  ->  a probability.
    ("woe", """
SELECT u.signal, u.value,
       ln( ((SUM(u.is_bad) + 0.5) / (MAX(tot.n_bad) + 0.5))
         / ((SUM(1 - u.is_bad) + 0.5) / (MAX(tot.n_good) + 0.5)) ) AS woe,
       COUNT(*) AS n_cal, SUM(u.is_bad) AS n_cal_bad
FROM (
    SELECT 'fan_out' AS signal, f_fan_out AS value, is_bad FROM account_features WHERE split = 'calibration'
    UNION ALL SELECT 'fan_in', f_fan_in, is_bad FROM account_features WHERE split = 'calibration'
    UNION ALL SELECT 'cycle', f_cycle, is_bad FROM account_features WHERE split = 'calibration'
    UNION ALL SELECT 'sg_end', f_sg_end, is_bad FROM account_features WHERE split = 'calibration'
    UNION ALL SELECT 'sg_mule', f_sg_mule, is_bad FROM account_features WHERE split = 'calibration'
    UNION ALL SELECT 'gather_scatter', f_gather_scatter, is_bad FROM account_features WHERE split = 'calibration'
    UNION ALL SELECT 'segment', f_segment, is_bad FROM account_features WHERE split = 'calibration'
) u
CROSS JOIN (SELECT SUM(is_bad) AS n_bad, SUM(1 - is_bad) AS n_good
            FROM account_features WHERE split = 'calibration') tot
GROUP BY u.signal, u.value
"""),
    ("account_risk", """
SELECT f.*, n.network_score, sg.woe AS segment_score,
       pr.prior + n.network_score + sg.woe AS logit,
       1.0 / (1.0 + exp(-(pr.prior + n.network_score + sg.woe))) AS risk_probability,
       ( CASE WHEN f.f_cycle = 1 THEN 'Part of ' || CAST(f.n_cycles AS STRING) || ' money cycle(s) (up to ' || CAST(f.max_cycle_len AS STRING) || ' hops) that return funds to the sender; ' ELSE '' END
      || CASE WHEN f.sg_as_source > 0 THEN 'Splits funds across mule accounts that re-converge (scatter-gather source); ' ELSE '' END
      || CASE WHEN f.sg_as_sink > 0 THEN 'Collects funds re-converging from several mule accounts (scatter-gather sink); ' ELSE '' END
      || CASE WHEN f.f_sg_mule = 1 THEN 'Acts as an intermediary (mule) in a scatter-gather scheme; ' ELSE '' END
      || CASE WHEN f.f_fan_out = 1 THEN 'Sent to ' || CAST(f.max_daily_fan_out AS STRING) || ' different accounts in a single day (fan-out); ' ELSE '' END
      || CASE WHEN f.f_fan_in = 1 THEN 'Received from ' || CAST(f.max_daily_fan_in AS STRING) || ' different accounts in a single day (fan-in); ' ELSE '' END
      || CASE WHEN f.f_gather_scatter = 1 THEN 'Collected from many and paid out to many on the same day (gather-scatter); ' ELSE '' END
      || CASE WHEN f.is_hub = 1 THEN 'High-volume hub account (likely merchant/payroll) - fan flags suppressed; ' ELSE '' END
      || CASE WHEN f.f_segment >= 2 THEN 'Transacts mainly in high-risk channels (avg. segment lift ' || CAST(ROUND(f.seg_lift_avg, 1) AS STRING) || 'x); ' ELSE '' END
       ) AS reasons
FROM account_features f
JOIN (
    SELECT f2.acct, SUM(w.woe) AS network_score
    FROM account_features f2
    JOIN woe w ON (w.signal = 'fan_out' AND w.value = f2.f_fan_out)
               OR (w.signal = 'fan_in' AND w.value = f2.f_fan_in)
               OR (w.signal = 'cycle' AND w.value = f2.f_cycle)
               OR (w.signal = 'sg_end' AND w.value = f2.f_sg_end)
               OR (w.signal = 'sg_mule' AND w.value = f2.f_sg_mule)
               OR (w.signal = 'gather_scatter' AND w.value = f2.f_gather_scatter)
    GROUP BY f2.acct
) n ON n.acct = f.acct
JOIN woe sg ON sg.signal = 'segment' AND sg.value = f.f_segment
CROSS JOIN (SELECT ln((SUM(is_bad) + 0.5) / (SUM(1 - is_bad) + 0.5)) AS prior
            FROM account_features WHERE split = 'calibration') pr
"""),
    ("account_ranked", """
SELECT r.*,
       ROW_NUMBER() OVER (ORDER BY r.logit DESC, r.acct) AS risk_rank,
       ROW_NUMBER() OVER (PARTITION BY r.split ORDER BY r.logit DESC, r.acct) AS rank_combined,
       ROW_NUMBER() OVER (PARTITION BY r.split ORDER BY r.network_score DESC, r.acct) AS rank_network,
       ROW_NUMBER() OVER (PARTITION BY r.split ORDER BY r.segment_score DESC, r.seg_lift_avg DESC, r.acct) AS rank_segment
FROM account_risk r
"""),
    # 14. Evaluation on HOLDOUT accounts only. Compares three ways to build an investigator watchlist:
    #     segment-only (Q1 channel rules), network-only (Q2 motifs) and combined (our scorecard).
    ("evaluation", """
SELECT k.k,
       CAST(SUM(CASE WHEN a.rank_combined <= k.k THEN a.is_bad ELSE 0 END) AS DOUBLE) / k.k AS precision_combined,
       CAST(SUM(CASE WHEN a.rank_network  <= k.k THEN a.is_bad ELSE 0 END) AS DOUBLE) / k.k AS precision_network_only,
       CAST(SUM(CASE WHEN a.rank_segment  <= k.k THEN a.is_bad ELSE 0 END) AS DOUBLE) / k.k AS precision_segment_only,
       CAST(SUM(a.is_bad) AS DOUBLE) / COUNT(*) AS base_rate,
       CAST(SUM(CASE WHEN a.rank_combined <= k.k THEN a.is_bad ELSE 0 END) AS DOUBLE) / SUM(a.is_bad) AS recall_combined,
       COUNT(*) AS n_holdout_accounts, SUM(a.is_bad) AS n_holdout_bad
FROM account_ranked a
CROSS JOIN (SELECT 50 AS k UNION ALL SELECT 100 UNION ALL SELECT 250 UNION ALL SELECT 500
            UNION ALL SELECT 1000 UNION ALL SELECT 2500) k
WHERE a.split = 'holdout'
GROUP BY k.k
"""),
    # 14b. TIME-BASED evaluation (the official figure). Accounts are scored using ONLY the learning days
    #      (before --train-until); we then check which holdout accounts touch laundering in the FUTURE.
    #      period 'official'  = train_until <= day < test_until  (HI-Small: 8-10 Sep, a realistic mix of days)
    #      period 'incl_tail' = every day from train_until on   (adds 11-18 Sep, almost all laundering tails)
    #      future_truth is supplied by the engine (empty when no train_until is given). See docs/decisions.md.
    ("evaluation_time", """
SELECT period, k.k,
       CAST(SUM(CASE WHEN rank_combined <= k.k THEN is_bad ELSE 0 END) AS DOUBLE) / k.k AS precision_combined,
       CAST(SUM(CASE WHEN rank_network  <= k.k THEN is_bad ELSE 0 END) AS DOUBLE) / k.k AS precision_network_only,
       CAST(SUM(CASE WHEN rank_segment  <= k.k THEN is_bad ELSE 0 END) AS DOUBLE) / k.k AS precision_segment_only,
       CAST(SUM(is_bad) AS DOUBLE) / COUNT(*) AS base_rate,
       CAST(SUM(CASE WHEN rank_combined <= k.k THEN is_bad ELSE 0 END) AS DOUBLE) / NULLIF(SUM(is_bad), 0) AS recall_combined,
       COUNT(*) AS n_holdout_accounts, SUM(is_bad) AS n_holdout_bad
FROM (
    SELECT per.period, a.rank_combined, a.rank_network, a.rank_segment,
           CASE WHEN per.period = 'official' THEN COALESCE(f.is_bad_official, 0)
                ELSE COALESCE(f.is_bad_all, 0) END AS is_bad
    FROM account_ranked a
    CROSS JOIN (SELECT 'official' AS period UNION ALL SELECT 'incl_tail') per
    LEFT JOIN future_truth f ON f.acct = a.acct
    WHERE a.split = 'holdout'
) z
CROSS JOIN (SELECT 50 AS k UNION ALL SELECT 100 UNION ALL SELECT 250 UNION ALL SELECT 500
            UNION ALL SELECT 1000 UNION ALL SELECT 2500) k
GROUP BY period, k.k
"""),
    ("signal_precision", """
SELECT w.signal, w.value, w.woe, w.n_cal, w.n_cal_bad,
       CAST(w.n_cal_bad AS DOUBLE) / w.n_cal AS precision_calibration
FROM woe w
"""),
    # 15. Which laundering typologies does the top-k watchlist catch? (uses the Patterns ground-truth file)
    ("pattern_coverage", """
SELECT pattern_type, COUNT(*) AS n_attempts, SUM(detected) AS n_detected,
       CAST(SUM(detected) AS DOUBLE) / COUNT(*) AS detection_rate
FROM (
    SELECT p.attempt_id, p.pattern_type,
           MAX(CASE WHEN w1.acct IS NOT NULL OR w2.acct IS NOT NULL THEN 1 ELSE 0 END) AS detected
    FROM patterns p
    LEFT JOIN (SELECT acct FROM account_ranked WHERE risk_rank <= {top_k}) w1 ON w1.acct = p.src_id
    LEFT JOIN (SELECT acct FROM account_ranked WHERE risk_rank <= {top_k}) w2 ON w2.acct = p.dst_id
    GROUP BY p.attempt_id, p.pattern_type
) d
GROUP BY pattern_type
"""),
    # 16. Serving tables for the dashboard and the streaming monitor
    ("watchlist", """
SELECT acct, risk_rank, risk_probability, logit, network_score, segment_score, reasons,
       max_daily_fan_out, max_daily_fan_in, n_cycles, max_cycle_len, sg_as_source, sg_as_sink, sg_as_mule,
       gather_scatter_day, seg_lift_avg, total_degree, is_hub, split, is_bad
FROM account_ranked
WHERE risk_rank <= {top_k}
"""),
    ("watchlist_edges", """
SELECT src_id, dst_id, n_txn, usd, first_ts, last_ts, any_laundering
FROM (
    SELECT p.*, ROW_NUMBER() OVER (PARTITION BY w.acct ORDER BY p.usd DESC) AS rn
    FROM pairs p
    JOIN (SELECT acct FROM account_ranked WHERE risk_rank <= 300) w
      ON w.acct = p.src_id OR w.acct = p.dst_id
) z
WHERE rn <= 40
"""),
]

# Tables persisted as outputs (everything else is an intermediate view)
OUTPUTS = ["watchlist", "watchlist_edges", "evaluation", "evaluation_time", "signal_precision", "pattern_coverage"]

DEFAULT_PARAMS = {
    "window_days": 3,              # laundering hops must follow each other within 3 days
    "pass_lo": 0.5,                # pass-through: 2nd hop moves 50%..120% of the 1st hop's amount
    "pass_hi": 1.2,
    "top_k": 1000,                 # watchlist size an investigation team could realistically work through
    "gs_k": 4,                     # gather-scatter: >= 4 in AND >= 4 out on the same day
    "sg_k": 3,                     # scatter-gather: >= 3 distinct mules
    # data-driven thresholds filled at runtime: fan_out_k, fan_in_k (99.9th pct, min 5), hub_degree (99.9th pct, min 50)
}


def render(sql, params):
    offsets = " UNION ALL ".join(f"SELECT {i} AS o" for i in range(int(params["window_days"]) + 1))
    return sql.format(offsets_sql=offsets, **params)


# Built by the engine from ALL transactions (not just the learning days) before the steps run.
# {train_until} / {test_until} are 'YYYY-MM-DD'; with no --train-until the engine creates it empty.
FUTURE_TRUTH_SQL = """
SELECT acct,
       MAX(CASE WHEN txn_date < '{test_until}' THEN is_laundering ELSE 0 END) AS is_bad_official,
       MAX(is_laundering) AS is_bad_all
FROM (SELECT src_id AS acct, txn_date, is_laundering FROM transactions_all WHERE txn_date >= '{train_until}'
      UNION ALL
      SELECT dst_id AS acct, txn_date, is_laundering FROM transactions_all WHERE txn_date >= '{train_until}') u
GROUP BY acct
"""
