# Raw AWS evidence

Records saved from AWS before teardown, behind the figures quoted in the README, `docs/` and the slides.
Collected with `bash infra/collect_evidence.sh <label>` unless noted. One folder per AWS account.
Times inside the files are UTC (Singapore = UTC+8).

```
raw_aws_output/
  learner-lab_465553375598/   first account: tuning before/after, 1 / 3 / 7-day window experiment (4 Nov)
  team_050752609589/          team account: official HI-Small (4 Nov) and HI-Large (4 and 7 Nov) runs
```

Inside each account folder, files keep the names AWS gives them:
`steps_<cluster>.json` (EMR steps, arguments, start/end), `cluster_<cluster>.json`, `driver_logs/<cluster>/…/stdout.gz`
(Spark driver output with per-step timings), `run_info/` (BQ2 parameters and step seconds), `stepfunctions/`,
`glue_runs_<job>.json`, `s3/` (pipeline outputs as written to S3), `athena/` (query + result), `ec2_streaming/`
(serving EC2's Kafka logs), `screenshots/` (taken by hand in the console).

## Learner Lab (`learner-lab_465553375598/`)

| File | Backs |
|---|---|
| `steps_j-06704321A7T11TFRQMW0.json`, `driver_logs/j-06704321A7T11TFRQMW0/stdout.gz` | Before tuning: BQ2 step 40 min 10 s; ranking step 669.9 s of 931.8 s |
| `steps_j-066507029D365FFVM9BN.json`, `run_info/analytics/HI-Small.json` | After tuning: BQ2 step 2 min 56 s; ranking step 15.9 s |
| `steps_j-01028977DXQQ53H5QK5.json`, `run_info/experiments/w1, w3, w7.json` | Window experiment: `--window-days 1 / 3 / 7`, path-join 9.5 / 11.3 / 15.7 s, job totals 120.7 / 124.1 / 129.7 s |

Not collected from this account: the window experiment's precision and schemes-found for 1 and 7 days.

## Team account (`team_050752609589/`)

| Folder / file | Backs |
|---|---|
| `stepfunctions/`, `glue_runs_*.json` | Whole pipeline: HI-Large 24 min 8 s (7 Nov), HI-Small 17 min 17 s (4 Nov); Glue ETL 7 min 45 s |
| `steps_*`, `cluster_*`, `driver_logs/`, `run_info/` | BQ2 EMR step HI-Small 2 min 48 s; job timers 178 s / 120 s; thresholds |
| `s3/analytics/q2_*` | BQ2 accuracy (67 %, 18.8 %, 71 %; HI-Small 3.0 %, 2.4 %, 64 %), schemes found (163 of 370) |
| `s3/analytics/q1_*`, `s3/analytics/run_log/` | BQ1 capture (76.7 % / 75.0 % at 5 %), segments at 5 % (58 / 45), Athena bytes and cost per query |
| `s3/dq_report/`, `s3/manifests/` | Row counts, duplicates, malformed and FX checks, file md5 |
| `athena/` | Daily volumes and laundering share, self-payments |
| `s3/streaming_alerts/`, `ec2_streaming/` | Kafka alerts per run; latest run (8 Nov): 300,000 in 96 s, median 64 ms; 190 hubs (consumer log) |
| `screenshots/` | See below |

### Screenshots (`team_050752609589/screenshots/`)

Taken by hand in the AWS console and the dashboard, 11 Nov 2026. The console shows Singapore time (UTC+8), so the
HI-Large run that the docs date **7 Nov (UTC)** appears as **8 Nov 01:40** on screen.

| Screenshot | What it shows |
|---|---|
| `stepfunctions_executions_list.png` | All five pipeline executions, including the two failed HI-Small attempts on 4 Nov; HI-Large 24:07.785, HI-Small 17:16.878 |
| `stepfunctions_HI-Large_2026-11-07_graph.png`, `stepfunctions_graph_detail_HI-Large_2026-11-07.png` | One-click run: every state green, duration 00:24:07.785, ETL → crawler → Athena Q1 → (EMR Q2 ∥ benchmark) |
| `glue_etl_runs.png` | Glue ETL on HI-Large: 10 × G.1X workers, 7 min 45 s, succeeded |
| `glue_catalog_tables.png` | The 15 tables in the `aml` Data Catalog database |
| `s3_bucket_zones.png`, `s3_curated_partitions.png` | Data-lake zones (raw / curated / analytics); curated HI-Large partitioned by `txn_date` (165 days) |
| `emr_clusters_terminated.png` | Every `aml-q2-*` cluster terminated after its run (transient EMR) |
| `emr_steps_HI-Small_2026-11-04.png` | BQ2 step 2 min 48 s on 1 primary + 2 core, with its arguments (`--window-days 3`) |
| `spark_ui_checkpoint_HI-Small.png` | Spark SQL queries named "checkpoint at …" (the tuning in action) |
| `spark_ui_stages_HI-Small_2026-11-04.png` | 92 completed, **104 skipped** stages (skipped = output already available, so Spark did not recompute it) |
| `athena_q2_evaluation_HI-Large.png` | `q2_evaluation_time` in Athena: official top 100 = 0.67, top 1,000 = 0.188, base rate 0.0467 |
| `athena_benchmark_csv_HI-Large.png`, `athena_benchmark_parquet_HI-Large.png` | Same query: CSV scans 15.88 GB, Parquet 65.97 MB (247×; the console uses 1 GB = 1,024³ bytes). Row counts differ by exactly the 150 duplicates the ETL removed; laundering counts are identical |
| `dashboard_bq1_HI-Large_redacted.png` | Dashboard served from the EC2 (`34.227.67.207:8501`): 76.7 % caught at 5 %, 58 segments |
| `dashboard_bq2_money_trail_redacted.png` | Watchlist and money-trail explorer on HI-Large |
| `dashboard_live_monitor.png` | Kafka Live monitor: 300,000 events, 3,071 alerts, 3.58 % precision, p50 64.3 ms |
| `dashboard_cost_performance.png` | CSV vs Parquet as the dashboard reports it: 247× / 10,091× (HI-Large), 250× / 1,189× (HI-Small) |
| `ec2_instances.png` | Serving EC2 `aml-serving`, t3.large, tagged `Project=aml-g2` (the tag teardown uses) |
| `ssm_kafka_topic.png` | Systems Manager Run Command output: topic `aml-transactions`, 3 partitions (no SSH) |
| `cost_explorer_2026-11-07.png` | 7 Nov (UTC): **USD 3.78** for the whole account, serving EC2 included |
| `cost_explorer_nov_month.png` | 1–11 Nov: **USD 18.12** for the whole team account, against the USD 250 budget |

`_redacted` copies blur the browser's bookmarks bar and profile icon; the originals are not committed.
