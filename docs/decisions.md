# Decision log

Decisions made while running the pipeline on AWS, with the evidence behind each. Newest first.

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
