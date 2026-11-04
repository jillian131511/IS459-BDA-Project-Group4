# Rubric and brief checklist (IS459 Project brief, G2)

Status key: ✅ done (and run on AWS where relevant) · 🔜 still to do on AWS · 👥 needs the team. Updated 4 Nov 2026.

## Deliverables
| Requirement (brief slide) | Where it is met | Status |
|---|---|---|
| 2 distinct business questions + who benefits and why (s5) | README, `docs/design.md` §1, deck slides 4-5 | ✅ |
| Extra data allowed if cited (s5) | Not needed: FX implied from the data itself; HI-Small `rows_without_fx_rate` = 0 | ✅ |
| One or more end-to-end AWS pipelines, ingestion to visualisation (s6) | `ingestion/` → `glue/` → `athena/` → `emr/` → `dashboard/`, orchestrated by `stepfunctions/`. HI-Small runs end to end on AWS in ~16 min; dashboard and HI-Medium next | ✅ / 🔜 |
| Architecture diagram per pipeline (s6) | `docs/architecture.png` (batch + speed layer in one diagram) | ✅ |
| No Marketplace; avoid Redshift, RDS, QuickSight (s6) | Athena + S3 + Streamlit; nothing purchased | ✅ |
| Budget USD 250 (s6) | Team account; a full HI-Small run costs well under USD 1 after tuning; per-query cost log in `analytics/run_log/` | ✅ / 🔜 |
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
| Presentation & demo (s12) | Crystal clear, seamless, no clarification needed, **plus at least one meaningful X-factor** | One-click Step Functions run; pre-recorded backup clips; dashboard tells the story tab by tab; four X-factors, each with a stated purpose |
| Problem solving & appropriateness (s13) | Challenging problem, very appropriate solution | Multi-hop network detection over up to 180M transfers is hard; every tool choice is justified against lecture content (`design.md` §3) |
| Working pipeline & usability (s14) | No bugs, efficient, intuitive | 8-test local suite; synthetic smoke test on AWS before real data; 7 bugs found on AWS and fixed (`docs/decisions.md`); idempotent deploy; results identical run to run; dashboard needs no instructions |
| Business value (s10) | Meaningful questions, clear beneficiaries, actionable insights | Budget slider: review 5 % of transfers, catch 75 % of laundering; watchlist top 100 is 64 % launderers vs 1 % at random; official precision measured on later days |
| X-factor (s11) | Clear purpose, not just complexity | Orchestration (repeatable, guaranteed EMR shutdown) · evidenced tuning (Q2 44 → 3.6 min, identical result) and the 1/3/7-day window experiment · CSV vs Parquet (250× less data scanned) · streaming if built |
| ML/AI only if meaningful (s11) | – | Transparent WoE scorecard; explainable alerts; no black box |
