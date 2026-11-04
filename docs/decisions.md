# Decision log

Decisions made while running the pipeline on AWS, with the evidence behind each. Newest first.

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
