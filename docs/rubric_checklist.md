# Rubric and brief checklist (IS459 Project brief, G2)

Status key: ✅ built and tested locally · 🔜 needs the AWS run · 👥 needs the team

## Deliverables
| Requirement (brief slide) | Where it is met | Status |
|---|---|---|
| 2 distinct business questions + who benefits and why (s5) | README, `docs/design.md` §1, deck slides 4-5 | ✅ |
| Extra data allowed if cited (s5) | Not needed: FX implied from the data itself; add ECB rates only if `rows_without_fx_rate` > 0 | ✅ |
| One or more end-to-end AWS pipelines, ingestion to visualisation (s6) | `ingestion/` → `glue/` → `athena/` → `emr/` → `dashboard/`, orchestrated by `stepfunctions/` | ✅ / 🔜 |
| Architecture diagram per pipeline (s6) | `docs/architecture.png` (batch + speed layer in one diagram) | ✅ |
| No Marketplace; avoid Redshift, RDS, QuickSight (s6) | Athena + S3 + Streamlit; nothing purchased | ✅ |
| Budget USD 250 (s6) | Estimate ~USD 40; per-query cost log in `analytics/run_log/` | ✅ / 🔜 |
| Week 9 pitch: questions, beneficiaries, pain points, pipeline, storage, processing, analyses, expected output, draft diagram (s8) | Deck slides 2-12 | ✅ |
| All members present at both pitches; every member demos one component in Week 13 (s8-9) | Workstream split on deck slide 12 | 👥 |
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
| Working pipeline & usability (s14) | No bugs, efficient, intuitive | Local test suite; synthetic smoke test on AWS before real data; idempotent deploy; defaults merged in Step Functions input; dashboard needs no instructions |
| Business value (s10) | Meaningful questions, clear beneficiaries, actionable insights | Alert-budget optimiser gives a direct decision; watchlist reasons make alerts actionable; precision is measured on holdout accounts |
| X-factor (s11) | Clear purpose, not just complexity | Orchestration (repeatable, guaranteed EMR shutdown) · streaming (catch money while it moves) · evidenced cost/performance (benchmark + run log) · decision-grade interactive visuals |
| ML/AI only if meaningful (s11) | – | Transparent WoE scorecard; explainable alerts; no black box |
