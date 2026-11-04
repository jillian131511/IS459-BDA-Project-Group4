# Decision log

Decisions made while running the pipeline on AWS, with the evidence behind each. Newest first.

## 2026-11-04 · Velocity rule fixed: hubs excluded, threshold 11 (chosen on a learning day)

**Change.** The live velocity rule now skips the batch layer's hub accounts (more than `hub_degree` counterparties
on the learning days; 190 accounts in HI-Small, published by the Q2 job to `analytics/serving/.../hubs/`), and its
threshold is 11 distinct payees in 24 h instead of 8.
**How it was chosen.** On a replay of **7 Sep** (a learning day) with hubs excluded, the threshold where the
velocity rule itself scores best (F1) was 11 — also the batch layer's own data-driven fan-out threshold.
**Measured once on 8 Sep** (grading day; 482,773 transfers, 539 laundering = 0.112 %), whole day, same scoring code
as the Kafka consumer (`streaming/monitor_core.py`):

| Live rules | Alerts | Share of transfers | Precision | vs random | Laundering transfers caught |
|---|---|---|---|---|---|
| Before: k = 8, hubs included | 71,834 | 14.9 % | 0.38 % | 3x | 273 of 539 (51 %) |
| **After: hubs excluded, k = 11** | **8,491** | **1.8 %** | **2.26 %** | **20x** | 192 of 539 (36 %) |

8.5x fewer alerts, each 6x more likely to be real, at the cost of catching 36 % instead of 51 % of laundering
transfers. The velocity rule itself adds little on top of the channel and watchlist rules (3,652 alerts, 13 real);
it is kept for the schemes those rules cannot see yet.

## 2026-11-04 · Speed layer works; the velocity rule is too noisy (fixed above)

Kafka (KRaft, 3 partitions) and the dashboard run on one t3.large (`infra/launch_serving_ec2.sh`). Replay of
HI-Small from the first grading day (8 Sep), 100,000 transfers in 285 s (about 350 a second), median delay from
transfer to alert 25 ms at low load and about 0.2 s at full speed. A few later-dated rows sit early in the raw
file; the producer now caps its pauses so they cannot stall the replay.

Honest evaluation on the replayed window (8 Sep 00:00-04:03: 93,577 transfers, 54 laundering = 0.058 %):

| Rule | Alerts | Truly laundering |
|---|---|---|
| Velocity (paid 8+ different accounts in 24 h) | 8,075 | 6 |
| Hot channel + watchlist rules | about 780 | 11 |
| All rules | 8,857 (9.5 % of transfers) | 17 (0.19 %, about 3x random); 17 of 54 laundering transfers caught |

The velocity rule fires mostly on busy ordinary accounts (merchants, payroll) - the "hubs" the batch layer already
leaves out of its fan-out flags. **Not tuned on these results**, because that would tune on the grading days.
Next step: exclude the batch layer's hub accounts from the velocity rule and set its threshold on a replay of a
learning day (7 Sep), then measure once on 8 Sep.

## 2026-11-04 · HI-Large is run in full (the pitch's 180 million transfers)

The team account has no USD 50 cap and allows 256 vCPUs, so HI-Large was run instead of HI-Medium. It has the
same shape as HI-Small: 97 normal days (1 Aug - 5 Nov 2022), then a tail (6 Nov - 12 Jan) of a few hundred to a
few thousand transfers a day, about 60 % laundering. Same rule as HI-Small: learn on the first 70 % of normal days
(1 Aug - 7 Oct), official grading on the rest (8 Oct - 5 Nov), tail reported separately.
Resources: Glue with 10 G.1X workers (6 min), EMR 9 x m5.2xlarge (Q2 compute 159 s). Before the run the
watchlist-edges step was rewritten from an OR join (a nested loop: about 30 billion comparisons at this size) to two
equi-joins, with identical output. Official top-1,000 precision 18.8 % against 4.7 % at random.

## 2026-11-04 · The pipeline runs in the team AWS account, not Learner Lab

The project belongs in the team account (`g2team4`, 050752609589, us-east-1), not an individual AWS Academy
Learner Lab. Learner Lab is limited to USD 50, 32 vCPUs and 4-hour sessions, and has the ready-made `LabRole`.
In the team account `infra/create_roles.sh` creates the equivalent (`aml-pipeline-role` + instance profile) and
tags the default VPC and its subnets, and every cluster carries the tag `for-use-with-amazon-emr-managed-policies=true`: the account's EMR roles use AWS's v2 managed policies, which refuse untagged clusters and networks (two failed runs taught us this). The data was re-downloaded there (same md5, so the same
file) and the official runs repeat there; because results are now identical run to run, every number above
carries over. The Learner Lab copy is deleted afterwards.

## 2026-11-04 · The Q2 time window stays at 3 days (evidence)

Same HI-Small data, same cluster (3 × m5.xlarge), only `--window-days` changed. Learn on 1–7 Sep.
Precision = share of the top-k holdout accounts that are laundering accounts (205,988 holdout accounts).

| Window | Q2 compute | Path-join step | Same days, top 500 | Same days, top 1,000 | Next days (8–10 Sep), top 1,000 | Typology attempts detected |
|---|---|---|---|---|---|---|
| 1 day | 120.7 s | 9.5 s | 12.0 % | 8.7 % | 2.2 % | 142 / 370 |
| **3 days** | 124.1 s | 11.3 s | **21.8 %** | **13.7 %** | 2.4 % | **163 / 370** |
| 7 days | 129.7 s | 15.7 s | 22.2 % | 14.1 % | 2.4 % | 157 / 370 |

Random picking: 1.0 % (same days) and 0.45 % (8–10 Sep).

**Reading.** Going from 1 to 3 days nearly doubles precision on the same days and finds the most laundering
attempts. Going from 3 to 7 days adds almost nothing (+0.4 points) and finds fewer attempts (more chance links
crowd the list), while the path-join step costs 39 % more. So 3 days is where the benefit levels off.
The window barely changes next-days precision (all ≈5× random): predicting which accounts launder *next* is
harder than spotting schemes already under way — reported honestly as the official figure.

## 2026-11-04 · Q2 accounts are split on the 2nd-to-last character of the account id

**Finding.** The scorecard learns on "calibration" accounts and is graded on "holdout" accounts, split by
the last hex character of the account id. On HI-Small, 483,675 account ids end in `0` and 13,317 in `1`,
so the holdout "half" was only 11,649 accounts (3 %) — a small and unusual group. The 2nd-to-last
character is evenly spread (≈31,000 accounts per hex digit), so the split now uses it.

**Effect.** All Q2 accuracy figures from before this change are discarded.

## 2026-11-04 · Q2 heavy steps are checkpointed, not cached (tuning)

**Observe.** First HI-Small run on EMR (3 × m5.xlarge): the Q2 step took 44 min; the ranking step
alone took 670 s of 932 s of step time while doing little real work.
**Hypothesise.** Each step's query plan carries every earlier step, so Spark spends its time re-planning
and matching cached plans rather than computing. (Adding *more* caching made it slower, which supports this.)
**Change one thing.** `--lineage checkpoint`: heavy steps are checkpointed to the cluster's HDFS, which
saves the result and cuts the plan back to "read this saved table".
**Measure.** Local SYN-Small, same code, only the switch changed: 593.5 s → 40.0 s, identical watchlist.
EMR HI-Small figures: see the experiment results.

A second finding on the way: the fan-in/fan-out thresholds used Spark's *approximate* percentile, which
returned 9, 10 or 11 for the same data depending on layout and changed the watchlist between runs. They now
use the exact percentile.

## 2026-11-04 · Q2 accuracy is graded on 8–10 Sep (HI-Small)

**Finding.** HI-Small spans 18 days, not 10. From 11 Sep onwards the simulator produces almost no normal
activity — only the tails of laundering schemes that started earlier:

| Days | Transfers per day | Share that is laundering |
|---|---|---|
| 1–10 Sep | 200,000 – 1,100,000 | ~0.1 % |
| 11–18 Sep | 11 – 396 | 50–70 % |

(Athena, `aml.transactions`, variant HI-Small, after Glue cleaning.)

**Decision.** Q2 learns from **1–7 Sep** and is graded on **8–10 Sep** — the official figure.
Accuracy including 11–18 Sep is also computed and shown as a second figure, clearly labelled.

**Why.** On 11–18 Sep almost every active account is laundering, so any method scores well there; the
schemes also started inside the learning period, so the same accounts reappear. Grading on those days
would overstate how well the watchlist works on a realistic day. The 11–18 Sep data is **not** deleted:
it stays in S3, in Q1 and in detection — it is only left out of the official grade.

**Slide wording.** "Graded on 8–10 Sep, a realistic mix of days: X % precision. Including 11–18 Sep it
rises to Y %, but those days are almost entirely laundering tails, so X is our honest figure."

## 2026-11-04 · Exchange rates come from the data itself

Glue derives one fixed rate per currency from transfers with US Dollars on one side (median). On
HI-Small all 15 currencies, including Bitcoin, get a rate and `dq_report.rows_without_fx_rate = 0`, so no
external rate table is needed. Live rates were rejected: the transactions are simulated and dated 2022.

## 2026-11-04 · Self-transfers are kept (for now)

591,212 HI-Small rows (11.6 %) are an account paying itself; only 11 are laundering. They are kept in
all figures and mentioned on the slides. Removing them would change every Q1 number, so it is a team
decision.

## 2026-11-04 · HI-Large is optional

The pitch promised HI-Large (180M rows) but also asked the professor "HI-Large or HI-Medium?". The
account has a US$50 budget, not US$250. Plan: HI-Medium is the guaranteed headline; one HI-Large attempt
with a hard time/cost cap if time allows.
