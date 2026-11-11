# Decision log

Decisions made while running the pipeline on AWS, with the evidence behind each. Newest first.
BQ1 / BQ2 are the two business questions (channels / networks), named as on the dashboard.

## 2026-11-08 · Kafka replay sorted by event time (local fix; EC2 verification pending)

The raw CSV is not strictly chronological. Kafka preserves publication order within a partition, but cannot
sort event timestamps; the velocity rule's deque requires chronological events per sender. The producer now
filters the requested interval, sorts it on temporary disk with SQLite, and applies `--max-events` after sorting.
Equal timestamps retain file order. It no longer skips far-ahead rows or caps replay pauses. Sender keys use
the same bank/account normalisation as the consumer; only one request per connection is in flight to preserve
order across retries. Temporary disk is needed for the selected interval and its sort; preparation reads the
entire source before publishing, even for a small event limit.

Validation: `python -m unittest discover -s tests -p test_replay_order.py -v` passed locally. The regression checks
shuffled timestamps, interval boundaries, equal-time ordering, limits after sorting and expiry of old velocity
events. This is not an EC2/Kafka validation. Member 6 (Video B / Kafka demo) must deploy the updated producer
and rerun `run_live_demo.sh`, which resets the topic and starts a fresh consumer. No batch rerun is needed.
Re-measure live alert counts and latency before quoting them for this version; earlier Kafka figures below
describe the old replay. Consumer latency starts at `produced_at`, excluding replay preparation and pacing.

Proposal review also identified outstanding qualifications, not implemented fixes: BQ1 selects a prefix of
Wilson-ranked segments rather than solving a maximum-capture optimisation; its curve is historical capture.
BQ2's segment-risk feature uses labels from all accounts in the learning period, so its account holdout is not
fully label-independent, although official grading uses later days. EMR has explicit Spark-failure cleanup and
an idle-timeout fallback, but parallel-branch failure and termination failure prevent an unconditional shutdown
guarantee. Money trails cover the top 300 accounts, not the entire top-1,000 watchlist.

## 2026-11-08 · Bank routes ranked by the Wilson lower bound; heatmap states its coverage

The per-bank route chart ranked routes by raw lift, so a route with 1 laundering transfer in 50 (20x) beat one with
500 in 50,000 (10x). It now ranks by the 95 % Wilson lower bound, the same rule as the monitoring rules (the
proposal's "worst-to-first, held back until there's enough to be sure"); bars show that cautious lift, labels the
raw lift. The heatmap picks banks by laundered value and colours cells by risk; its caption now says so and states
the share of laundered value its cells carry (44 % on SYN-Small; the rest is reachable in the per-bank tab). Each
route view has a CSV button below it, like the other heatmaps (the grid's routes, one bank's routes, all routes),
and clicking a heatmap cell switches to that bank's route chart.

## 2026-11-08 · Checked against the Week 9 proposal: bank-route heatmap added, full 1,000-account watchlist

The proposal promised a heatmap by bank-to-bank route and a 1,000-account watchlist. The dashboard had only a
corridor table and showed the first 300 watchlist accounts. Both are display changes on existing Athena tables
(`q1_bank_corridors`, `q2_watchlist` already holds 1,000 accounts), so no pipeline rerun was needed. The route
heatmap covers the 20 banks sending x the 20 receiving the most laundered value (the full matrix would be thousands
wide on HI-Large and almost empty); clicking a cell opens that sender in the per-bank route chart. CSV downloads
were added for every heatmap and the route views. All click-throughs (watchlist row, money-trail node, heatmap
cell, route bar) were checked in a browser. A sidebar link to the Kaggle dataset was tried and dropped: the README
already cites the source.

## 2026-11-08 · Local replay uses the same live rules as Kafka; graph labels never overlap

`local/replay_local.py` still used the old velocity rule (8 payees, hubs included), so local replays scored
differently from the Kafka consumer. `local/run_local.py` now writes `hubs.csv` with the same rule as the EMR job,
and the replay uses it with threshold 11 (on SYN-Small: 15 hubs excluded). The money-trail graph places labels
before drawing (flagged accounts first, then the largest flows), trying the 8 positions around each dot; a label
with no free spot is shortened or left to the hover, and the caption says how many. Flagged accounts' labels also
show what they paid or received.

## 2026-11-08 · BQ2 laid out for investigators; click an account to open its trail

The BQ2 tab now leads with the investigator tools (watchlist and money trail, in one box) and ends with the
evidence ("How the watchlist is scored and graded"), captioned as unaffected by the search or filter.
Clicking a node in the money-trail graph, or a row in the watchlist table, opens that account's trail, as long as
it is in the top 300 (the accounts with published trails) and matches the watchlist search and graded-half filter;
otherwise a note says why and the selection stays. Graph clicks are ported from the `dashboard-bq1-heatmaps` branch.
Needs Streamlit 1.65+ (chart and table selection events; keyed tabs so a heatmap click opens the route chart).

Two bugs fixed on the way, both when the graded-half filter removes the selected account: the dropdown kept showing
the old account while the graph moved on (the picker is now set explicitly), and the table's tick stayed on the
same row number, now a different account (the tick is cleared whenever the account changes outside the table).

## 2026-11-08 · BQ1 lists every monitoring rule inside the budget

The alert-budget optimiser used to show only the first 25 segments that fit the chosen review budget, while the
"Segments to monitor" figure counted all of them (e.g. 58 at 5 % on HI-Large). The table now lists every segment inside the
budget, under a heading that states the count ("Monitoring rules at this budget · 58 segments"). The slider and
everything it drives sit in one box; the heatmaps and bank routes below are labelled as covering all transfers,
independent of the budget.

## 2026-11-04 · Velocity rule fixed: hubs excluded, threshold 11 (chosen on a learning day)

**Change.** The live velocity rule now skips the batch layer's hub accounts (more than `hub_degree` counterparties
on the learning days; 190 accounts in HI-Small, published by the BQ2 job to `analytics/serving/.../hubs/`), and its
threshold is 11 distinct payees in 24 h instead of 8.
**How it was chosen.** On a replay of **7 Sep** (a learning day) with hubs excluded, the threshold where the
velocity rule itself scores best (F1) was 11 — also the batch layer's own data-driven fan-out threshold.
**Measured once on 8 Sep with the local replay** (`local/replay_local.py`; grading day; 482,771 transfers, 539
laundering = 0.112 %), whole day, same scoring code as the Kafka consumer (`streaming/monitor_core.py`). This is a
local measurement, not an AWS run; the live EC2 runs use the same "after" settings (consumer log: velocity k=11,
190 hubs excluded):

| Live rules | Alerts | Share of transfers | Precision | vs random | Laundering transfers caught |
|---|---|---|---|---|---|
| Before: k = 8, hubs included | 71,834 | 14.9 % | 0.38 % | 3x | 273 of 539 (51 %) |
| **After: hubs excluded, k = 11** | **8,491** | **1.8 %** | **2.26 %** | **20x** | 192 of 539 (36 %) |

8.5x fewer alerts, each 6x more likely to be real, at the cost of catching 36 % instead of 51 % of laundering
transfers. The velocity rule itself adds little on top of the channel and watchlist rules (3,652 alerts, 13 real);
it is kept for the schemes those rules cannot see yet.

Historical replay, superseded by the 8 Nov ordering fix (HI-Small from 8 Sep 00:00, 100,000 transfers in 36 s; the producer then skipped
rows more than an hour ahead of the stream's clock — 1,001 out-of-order rows, mostly laundering, that would otherwise
inflate the alert precision on the Live monitor tab): 875 alerts (0.9 % of transfers), 5 truly laundering (0.57 %, about 10x
the 0.058 % base rate of that 4.5-hour window), median 0.16 s from transfer to alert. (The 875 and 5 are in the alerts
saved from S3, `raw_aws_output/team_050752609589/s3/streaming_alerts/`; the run's timing log was not kept.) The whole-day figure above
(2.26 %, 20x) is the one to quote; the replay window only holds 54 laundering transfers.

## 2026-11-04 · Speed layer works; the velocity rule is too noisy (fixed above)

Kafka (KRaft, 3 partitions) and the dashboard run on one t3.large (`infra/launch_serving_ec2.sh`). Replay of
HI-Small from the first grading day (8 Sep), 100,000 transfers in 285 s (about 350 a second), median delay from
transfer to alert 25 ms at low load and about 0.2 s at full speed. A few later-dated rows sit early in the raw
file; the producer then capped its pauses. This behaviour was superseded by the 8 Nov ordering fix.
(These 4 Nov speed figures come from the run's console output at the time; its logs were overwritten by later runs
and were not kept.)

**Latest measurement (8 Nov 21:03, after the ordering fix, same t3.large).** HI-Small from 8 Sep 00:00, 300,000
transfers in 96 s (about 3,100 a second), median delay from transfer to alert 64 ms, 95th percentile 151 ms;
3,071 alerts (1.0 % of transfers), 3.6 % of them truly laundering. Source: the serving EC2's producer and consumer
logs, `raw_aws_output/team_050752609589/ec2_streaming/`. Quote these; the 4 Nov figures above are kept for the record.

Honest evaluation on the replayed window (8 Sep 00:00-04:03: 93,577 transfers, 54 laundering = 0.058 %):

| Rule | Alerts | Truly laundering |
|---|---|---|
| Velocity (paid 8+ different accounts in 24 h) | 7,911 | 6 |
| Hot channel + watchlist rules | 765 | 11 |
| All rules | 8,676 (9.3 % of transfers) | 17 (0.20 %, about 3x random); 17 of 54 laundering transfers caught |

(Recounted from the alerts the consumer wrote to S3, `raw_aws_output/team_050752609589/s3/streaming_alerts/`, events
dated 8 Sep 00:00-04:03.)

The velocity rule fires mostly on busy ordinary accounts (merchants, payroll) - the "hubs" the batch layer already
leaves out of its fan-out flags. **Not tuned on these results**, because that would tune on the grading days.
Next step: exclude the batch layer's hub accounts from the velocity rule and set its threshold on a replay of a
learning day (7 Sep), then measure once on 8 Sep.

## 2026-11-04 · HI-Large is run in full (the pitch's 180 million transfers)

The team account has no USD 50 cap and allows 256 vCPUs, so HI-Large was run instead of HI-Medium. It has the
same shape as HI-Small: 97 normal days (1 Aug - 5 Nov 2022), then a tail (6 Nov - 12 Jan) of 2 to 2,192
transfers a day, about 60 % laundering. Same rule as HI-Small: learn on the first 70 % of normal days
(1 Aug - 7 Oct), official grading on the rest (8 Oct - 5 Nov), tail reported separately.
Resources: Glue with 10 G.1X workers (7 min 45 s), EMR 9 x m5.2xlarge (BQ2 compute 159 s; the 7 Nov re-run, same parameters and results, 178 s). Before the run the
watchlist-edges step was rewritten from an OR join (a nested loop: about 30 billion comparisons at this size) to two
equi-joins, with identical output. Official top-1,000 precision 18.8 % against 4.7 % at random.

## 2026-11-04 · The pipeline runs in the team AWS account, not Learner Lab

The project belongs in the team account (`g2team4`, 050752609589, us-east-1), not an individual AWS Academy
Learner Lab. Learner Lab is limited to USD 50, 32 vCPUs and 4-hour sessions, and has the ready-made `LabRole`.
In the team account `infra/create_roles.sh` creates the equivalent (`aml-pipeline-role` + instance profile) and
tags the default VPC and its subnets, and every cluster carries the tag `for-use-with-amazon-emr-managed-policies=true`: the account's EMR roles use AWS's v2 managed policies, which refuse untagged clusters and networks (two failed runs taught us this). The data was re-downloaded there (same md5, so the same
file) and the official runs repeat there; because results are now identical run to run, every number above
carries over. The Learner Lab copy is deleted afterwards.

## 2026-11-04 · The BQ2 time window stays at 3 days (evidence)

Same HI-Small data, same cluster (3 × m5.xlarge), only `--window-days` changed. Learn on 1–7 Sep.
Precision = share of the top-k holdout accounts that are laundering accounts (205,988 holdout accounts).

| Window | BQ2 compute | Path-join step | Same days, top 500 | Same days, top 1,000 | Next days (8–10 Sep), top 1,000 | Typology attempts detected |
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

## 2026-11-04 · BQ2 accounts are split on the 2nd-to-last character of the account id

**Finding.** The scorecard learns on "calibration" accounts and is graded on "holdout" accounts, split by
the last hex character of the account id. On HI-Small, 483,675 account ids end in `0` and 13,317 in `1`,
so the holdout "half" was only 11,649 accounts (3 %) — a small and unusual group. The 2nd-to-last
character is evenly spread (≈31,000 accounts per hex digit), so the split now uses it.

**Effect.** All BQ2 accuracy figures from before this change are discarded.

## 2026-11-04 · BQ2 heavy steps are checkpointed, not cached (tuning)

**Observe.** First HI-Small run on EMR (3 × m5.xlarge, Learner Lab account): the BQ2 step took 40 min 10 s
(EMR step time); the ranking step alone took 670 s of 932 s of timed step time while doing little real work.
**Hypothesise.** Each step's query plan carries every earlier step, so Spark spends its time re-planning
and matching cached plans rather than computing. (Adding *more* caching made it slower, which supports this.)
**Change one thing.** `--lineage checkpoint`: heavy steps are checkpointed to the cluster's HDFS, which
saves the result and cuts the plan back to "read this saved table".
**Measure.** Local SYN-Small, same code, only the switch changed: 593.5 s → 40.0 s, identical watchlist.
On EMR (HI-Small, 3 × m5.xlarge) the BQ2 step went from 40 min 10 s to 2 min 56 s (EMR step time, about
14×), and the ranking step from 669.9 s to 15.9 s. The same tuned code later took 2 min 48 s in the team account.
The EMR before/after is not a one-variable test: the same release also switched to exact thresholds (below) and
added later-days grading, and the fan-in threshold moved from 25 to 26, so the EMR watchlists are not identical.
The one-variable evidence is the local run above. Raw AWS records: `raw_aws_output/`.

A second finding on the way: the fan-in/fan-out thresholds used Spark's *approximate* percentile, which
returned 9, 10 or 11 for the same data depending on layout and changed the watchlist between runs. They now
use the exact percentile.

## 2026-11-04 · BQ2 accuracy is graded on 8–10 Sep (HI-Small)

**Finding.** HI-Small spans 18 days, not 10. From 11 Sep onwards the simulator produces almost no normal
activity — only the tails of laundering schemes that started earlier:

| Days | Transfers per day | Share that is laundering |
|---|---|---|
| 1–10 Sep | 200,000 – 1,100,000 | ~0.1 % |
| 11–18 Sep | 11 – 396 | about 60 % (57–73 % by day) |

(Athena, `aml.transactions`, variant HI-Small, after Glue cleaning.)

**Decision.** BQ2 learns from **1–7 Sep** and is graded on **8–10 Sep** — the official figure.
Accuracy including 11–18 Sep is also computed and shown as a second figure, clearly labelled.

**Why.** On 11–18 Sep almost every active account is laundering, so any method scores well there; the
schemes also started inside the learning period, so the same accounts reappear. Grading on those days
would overstate how well the watchlist works on a realistic day. The 11–18 Sep data is **not** deleted:
it stays in S3, in BQ1 and in detection — it is only left out of the official grade.

**Slide wording.** "Graded on 8–10 Sep, a realistic mix of days: X % precision. Including 11–18 Sep it
rises to Y %, but those days are almost entirely laundering tails, so X is our honest figure."

## 2026-11-04 · Exchange rates come from the data itself

Glue derives one fixed rate per currency from transfers with US Dollars on one side (median). On
HI-Small all 15 currencies, including Bitcoin, get a rate and `dq_report.rows_without_fx_rate = 0`, so no
external rate table is needed. Live rates were rejected: the transactions are simulated and dated 2022.

## 2026-11-04 · Self-transfers are kept (for now)

591,212 HI-Small rows (11.6 %) are an account paying itself; only 11 are laundering. They are kept in
all figures and mentioned on the slides. Removing them would change every BQ1 number, so it is a team
decision.

## 2026-11-04 · HI-Large is optional (superseded: HI-Large was run in full in the team account, see above)

The pitch promised HI-Large (180M rows) but also asked the professor "HI-Large or HI-Medium?". The
account has a US$50 budget, not US$250. Plan: HI-Medium is the guaranteed headline; one HI-Large attempt
with a hard time/cost cap if time allows.
