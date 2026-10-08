# Design rationale (with IS459 lecture references)

Lecture references point to the IS459 decks in the Project knowledge base, as *Week N slide S*.

## 1. Business questions

**Q1 · Where should the AML monitoring team spend its limited review capacity?**
* Beneficiary: transaction-monitoring analysts and the Head of AML Compliance, who set rules and alert volumes.
* Pain point: analyst hours are fixed; static thresholds flood queues with innocent alerts.
* Analysis: laundering rate, lift and 95 % Wilson lower bound per segment (payment format × payment currency ×
  cross-bank × cross-currency × amount band in USD); capture curve (share of transactions reviewed vs share of
  laundering caught when segments are monitored in risk order); time-of-week profile; bank-to-bank corridors.
* Output: alert-budget optimiser ("we can review X % of transfers, which rules catch the most?").

**Q2 · Which accounts are moving the money, and why do we think so?**
* Beneficiary: financial-crime investigators deciding which accounts to escalate.
* Pain point: single-transaction rules miss multi-hop schemes; unexplained alerts are slow to triage.
* Analysis: time-respecting graph motifs over the full transaction graph - daily fan-out / fan-in, 2-, 3- and
  4-hop cycles closing within a window, scatter-gather (>= 3 mules), gather-scatter; combined with the Q1 segment
  risk of each account's transactions in a weight-of-evidence (WoE) scorecard.
* Output: top-1,000 watchlist with risk probability and plain-English reasons; money-trail explorer.

The two questions are distinct: Q1 is about **channels** (where to look), Q2 about **entities and networks** (who
to investigate). Q2 re-uses Q1's output as one of its signals, which the evaluation shows is worth doing.

## 2. Data assessment (5 Vs - Week 3 slides 5-16)

| V | Observation | Design consequence |
|---|---|---|
| Volume | 5M (HI-Small) to 180M (HI-Large) rows | Spark (Glue, EMR); Parquet + partitioning; Athena for aggregation |
| Velocity | Delivered as files; real banks see a continuous stream | Batch layer + Kafka speed layer replaying unseen days |
| Variety | Transactions CSV + laundering-attempts text file; many currencies incl. Bitcoin | Separate parsers; FX normalisation to USD |
| Veracity | Header repeats `Account`; mixed currencies; labels are synthetic | Explicit schema at read time; DQ report (rows in / dropped / without FX); USD via rates implied by the data |
| Value | Labels + typologies available | We can *measure* precision and typology coverage, not just claim them |

**Assumptions to verify on the real files (flagged, not guessed):** timestamp format `yyyy/MM/dd HH:mm`; bank ids may
be zero-padded in the Patterns file; enough USD-paired cross-currency transfers exist to imply each currency's
rate (rows without a rate are counted in `dq_report.rows_without_fx_rate`; if non-zero, add an external FX table).

## 3. Architecture decisions and trade-offs

| Decision | Alternatives considered | Why this choice | Lecture |
|---|---|---|---|
| S3 data lake with raw / curated / analytics zones | Load straight into a warehouse | Keep raw as delivered, schema-on-read, cheap storage, re-processable | W4 s6, s30-33 |
| Glue ETL for preparation | EMR for everything | Serverless, no cluster to manage for a linear ETL | W4 s42; W6 s33 |
| Parquet, Snappy, partitioned by variant/date | Keep CSV | Columnar + partition pruning = fewer bytes scanned = cheaper/faster Athena (measured by our benchmark) | W2 s70-73; W5 s36 |
| Glue crawler for curated tables; DDL + partition projection for analytics tables | Crawl everything | Crawler mirrors the lab; projection needs no re-crawl for new variants | W4 Glue lab; W5 s39 |
| Athena for Q1 | Redshift | Serverless SQL, pay per scan; brief asks to avoid Redshift | W5 s36 |
| EMR (transient) for Q2 | Glue, Athena | Multi-hop self-joins are iterative and memory-heavy; Spark caches intermediates; EMR gives control of size | W6 s28, s33-37 |
| Step Functions | Manual runs, Airflow (MWAA) | Native Glue/EMR/Athena integrations, cheap, retries, guaranteed EMR termination | W4 s38 |
| Kafka on EC2 | Kinesis, MSK | Taught in W7; partitions keyed by sender keep per-account order for the velocity window | W7 s23-24 |
| Streamlit + boto3 → Athena | QuickSight | Brief asks to avoid QuickSight; lab pattern; only small aggregates leave Athena | W5 s37 |

**Batch vs streaming.** Graph motifs need history (days of transfers), so they are batch; the per-transaction
decision ("alert now?") needs low latency, so it is streaming. The streaming layer does not re-learn anything; it
applies batch outputs (watchlist, segment risk, FX) plus one stateful rule (24 h velocity window) that only a
stream can evaluate in time.

**Scalability / fault tolerance.** HDFS-style replication is replaced by S3 durability; Spark recomputes lost
partitions; Step Functions retries Glue concurrency errors and always terminates EMR; Kafka can scale consumers
up to the number of partitions (3).

## 4. Q2 method details

* Hub accounts (total degree above the 99.9th percentile, min 50) are excluded from path joins - they are
  merchants/payroll-like and would explode the joins - but still get fan-in/fan-out statistics.
* Thresholds are data-driven: fan-out/fan-in = 99.9th percentile of daily distinct counterparties (min 5).
* Two-hop paths are built from DAILY account-to-account flows with a day-offset table, so the 3-day time window is
  an equi-join on (account, day): work grows with an account's daily activity, not its lifetime activity, which
  keeps HI-Large tractable on a small EMR cluster.
* A path only counts if the second hop forwards 50-120 % of the first hop's amount ("pass-through", the signature
  of layering). On the synthetic test data this lifted cycle-flag precision from 5 % to 39 %.
* Cycles must close within 2x / 3x the 3-day window for 3- / 4-hop cycles.
* Scorecard: `logit = prior + sum(WoE(flag value))`, `WoE = ln(P(value | laundering account) / P(value | normal account))`
  with +0.5 smoothing, learned on calibration accounts only (even last hex digit of the account id).
  `risk_probability = 1 / (1 + e^-logit)`.
* Evaluation on holdout accounts: precision@k for k in {50 ... 2,500} for combined, network-only, segment-only and
  random; typology coverage of the top-k watchlist using the Patterns file.
* Limitations: naive-Bayes-style WoE double-counts correlated flags (ranking still valid, probabilities optimistic);
  cycles longer than 4 hops are not enumerated (cost grows exponentially); labels are synthetic.

## 5. Streaming rules (speed layer)

1. Watchlist: sender or receiver on the Q2 watchlist with probability >= 0.5 **and** the transaction is in a segment
   with lift >= 2 (prevents a watchlisted account's ordinary payments flooding the queue).
2. Hot segment: segment lift >= 20 with Wilson lower bound > 0.
3. Velocity: sender has paid >= 8 distinct accounts within the last 24 h of event time.

The consumer reports alert rate, alert precision (from labels, for the demo only) and p50/p95 latency.
