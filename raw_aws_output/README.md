# Raw AWS evidence

Records saved from AWS before teardown, behind the figures quoted in the README, `docs/` and the slides.
Collected with `infra/collect_evidence.sh` unless noted. Times inside the files are UTC (Singapore = UTC+8).

| Folder / file | Account | What it backs |
|---|---|---|
| `steps_j-*.json`, `cluster_j-*.json`, `run_info_*.json`, `pre_tuning_driver_stdout.gz` (top level) | Learner Lab (465553375598) | Tuning before/after (40 min 10 s → 2 min 56 s; ranking step 669.9 s → 15.9 s), window experiment run times and arguments (1 / 3 / 7 days) |
| `050752609589/stepfunctions/`, `glue_runs_*.json` | Team | Whole-pipeline times (HI-Large 24 min 8 s on 7 Nov, HI-Small 17 min 17 s on 4 Nov), Glue ETL times |
| `050752609589/steps_*`, `cluster_*`, `driver_logs/`, `run_info/` | Team | BQ2 EMR step times (HI-Small 2 min 48 s), job timers (178 s / 120 s), thresholds |
| `050752609589/s3/analytics/q2_*` | Team | BQ2 accuracy (67 %, 18.8 %, 71 %; HI-Small 3.0 %, 2.4 %, 64 %), schemes found (163 of 370) |
| `050752609589/s3/analytics/q1_*`, `run_log/` | Team | BQ1 capture (76.7 % / 75.0 % at 5 %), segments at 5 % (58 / 45), Athena bytes and cost per query |
| `050752609589/s3/dq_report/`, `s3/manifests/` | Team | Row counts, duplicates, malformed and FX checks, file md5 |
| `050752609589/athena/` | Team | Daily volumes and laundering share, self-payments (SQL saved next to each result) |
| `050752609589/s3/streaming_alerts/`, `ec2_streaming/` | Team | Kafka alerts per run; latest run's speed and latency (300,000 in 96 s, median 64 ms) |
| `050752609589/screenshots/spark_ui_stages_HI-Small_2026-11-04.png` | Team | Spark UI of the tuned HI-Small run (4 Nov 12:43 UTC): 92 completed, **104 skipped** stages (skipped = output already available, so Spark did not recompute it) |
| `050752609589/screenshots/cost_explorer_2026-11-07.png` | Team | Cost on 7 Nov, the recorded HI-Large run day: **USD 3.78** for the whole account, serving EC2 included |

Screenshots were taken by hand in the AWS console (11 Nov 2026).
