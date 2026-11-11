# Rubric and brief checklist (IS459 Project brief, G2)

Status key: ✅ done (and run on AWS where relevant) · 🔜 still to do on AWS · 👥 needs the team. Updated 8 Nov 2026.

**8 Nov review qualifications:** the Kafka timestamp-ordering fix has passed a local regression test, but its
EC2 deployment and fresh replay are pending (Member 6 / Video B); existing Kafka measurements predate it.
No batch rerun is needed for that fix. The dashboard's budget optimiser selects a Wilson-ranked prefix and
reports historical capture, not a guaranteed maximum. BQ2's BQ1 segment-risk feature uses all learning-period
accounts' labels, so account holdout is not fully label-independent; official grading uses later days.
EMR cleanup handles Spark success/failure and has an idle-timeout fallback, but shutdown is not unconditional.
The top-1,000 watchlist has money trails for the top 300 accounts.

The proposal's four X factors are **orchestration**, **near real-time**, **cost/performance evidence**, and
**decision-grade visuals** (budget slider plus clickable money trails). Spark tuning is additional evidence,
not a replacement for the fourth X factor. See `docs/decisions.md` for the implementation and pending checks.

## Deliverables
| Requirement (brief slide) | Where it is met | Status |
|---|---|---|
| 2 distinct business questions + who benefits and why (s5) | README, `docs/design.md` §1, deck slides 4-5 | ✅ |
| Extra data allowed if cited (s5) | Not needed: FX implied from the data itself; HI-Small `rows_without_fx_rate` = 0 | ✅ |
| One or more end-to-end AWS pipelines, ingestion to visualisation (s6) | `ingestion/` → `glue/` → `athena/` → `emr/` → `dashboard/`, orchestrated by `stepfunctions/`. HI-Small (17 min 17 s, 4 Nov) and HI-Large (24 min 8 s, 7 Nov) run end to end on AWS; dashboard and Kafka on the serving EC2 | ✅ |
| Architecture diagram per pipeline (s6) | `docs/architecture_light.png` / `architecture_dark.png` (batch + speed layer in one diagram; theme-aware in the dashboard) | ✅ |
| No Marketplace; avoid Redshift, RDS, QuickSight (s6) | Athena + S3 + Streamlit; nothing purchased | ✅ |
| Budget USD 250 (s6) | Team account; a full HI-Small run costs well under USD 1 after tuning, an HI-Large run about USD 2; per-query cost log in `analytics/run_log/` | ✅ / 🔜 |
| Week 9 pitch: questions, beneficiaries, pain points, pipeline, storage, processing, analyses, expected output, draft diagram (s8) | Deck slides 2-12 | ✅ |
| All members present at both pitches; every member demos one component in Week 13 (s8-9) | Owners per component in `docs/demo_and_video_scripts.md` §0 | 👥 |
| Individual contributions slide (s9) | Week 13 deck | 👥 |
| Video: set up pipeline end-to-end, narrated (s7) | `docs/demo_and_video_scripts.md` §2 | 🔜 |
| Video: run pipeline end-to-end, narrated (s7) | `docs/demo_and_video_scripts.md` §3 | 🔜 |
| Videos on YouTube, linked in appendix; source code uploaded / linked (s7) | GitHub repo + appendix slide | 👥 |
| Delete all AWS resources after Week 13 (s9) | `infra/teardown.sh` | ✅ |

## Grading rubric → how we aim for A+
| Rubric line | What A+ needs | Our evidence |
|---|---|---|
| Presentation & demo (s12) | Crystal clear, seamless, no clarification needed, **plus at least one meaningful X-factor** | One-click Step Functions run; backup clips planned; four proposal X-factors implemented, with the replay ordering fix awaiting EC2 verification |
| Problem solving & appropriateness (s13) | Challenging problem, very appropriate solution | Multi-hop network detection over up to 180M transfers is hard; every tool choice is justified against lecture content (`design.md` §3) |
| Working pipeline & usability (s14) | No bugs, efficient, intuitive | 11-test local suite (incl. a dashboard smoke run); synthetic smoke test on AWS before real data; bugs found on AWS fixed and logged with evidence (`docs/decisions.md`); idempotent deploy; results identical run to run; dashboard needs no instructions |
| Business value (s10) | Meaningful questions, clear beneficiaries, actionable insights | Budget slider: review 5 % of transfers, catch 76.7 % of laundering (HI-Large), with every segment to monitor listed and counted; official top-100 watchlist (HI-Large, graded on later days) is 67 % launderers vs 4.7 % at random |
| X-factor (s11) | Clear purpose, not just complexity | Orchestration (repeatable, explicit EMR cleanup plus idle fallback) · Kafka speed layer (live alerts, ordering fix locally tested; EC2 replay pending) · CSV vs Parquet (reported 250× less data scanned; retain run logs) · decision-grade visuals (budget slider, rule list, clickable money trails). Additional evidence: BQ2 tuning and the 1/3/7-day window experiment |
| ML/AI only if meaningful (s11) | – | Transparent WoE scorecard; explainable alerts; no black box |
