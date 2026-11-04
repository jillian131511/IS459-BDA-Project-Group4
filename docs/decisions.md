# Decision log

Decisions made while running the pipeline on AWS, with the evidence behind each. Newest first.

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
