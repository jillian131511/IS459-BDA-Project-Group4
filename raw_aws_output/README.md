# AWS evidence

Every figure in the README, `docs/` and the slides that comes from AWS is backed here by a raw record exported
from AWS or a screenshot of the AWS console. Collected on 11 Nov 2026, before teardown, with
`bash infra/collect_evidence.sh <label>`; screenshots taken by hand.

* **Two AWS accounts.** `learner-lab_465553375598/` is the first (Learner Lab) account, where BQ2 was tuned and
  the 1 / 3 / 7-day window experiment ran (4 Nov). `team_050752609589/` is the team account, which ran the
  official HI-Small (4 Nov) and HI-Large (4 and 7 Nov) pipelines.
* **Time zones.** Raw files are in UTC; the console screenshots show Singapore time (UTC+8). The HI-Large run the
  docs date **7 Nov (UTC)** appears as **8 Nov 01:40** in the screenshots.
* **Units.** The Athena console counts 1 GB as 1,024³ bytes; our logs count 1 MB as 10⁶ bytes (15.88 GB = 17,053 MB).

## Headline claims and where to see them

| Claim | Screenshot | Raw record |
|---|---|---|
| One-click pipeline: HI-Large **24 min 8 s** (7 Nov), HI-Small **17 min 17 s** (4 Nov) | [Step Functions executions](team_050752609589/screenshots/stepfunctions_executions_list.png), [graph](team_050752609589/screenshots/stepfunctions_HI-Large_2026-11-07_graph.png) | `team_…/stepfunctions/` |
| Glue ETL on HI-Large: 10 workers, **7 min 45 s** | [Glue job run](team_050752609589/screenshots/glue_etl_runs.png) | `team_…/glue_runs_aml-etl-transactions.json` |
| Data quality: 179,702,229 rows in, **150 duplicates** removed, 0 malformed, 0 without FX | [Athena CSV](team_050752609589/screenshots/athena_benchmark_csv_HI-Large.png) vs [Parquet](team_050752609589/screenshots/athena_benchmark_parquet_HI-Large.png) row counts differ by exactly 150 | `team_…/s3/dq_report/`, `s3/manifests/` |
| BQ1: reviewing the riskiest 5 % catches **76.7 %** of laundering, 58 segments (HI-Large) | [Dashboard BQ1](team_050752609589/screenshots/dashboard_bq1_HI-Large_redacted.png) | `team_…/s3/analytics/q1_segment_risk/` |
| BQ2: top 100 **67 %**, top 1,000 **18.8 %** vs 4.7 % random (HI-Large, official days) | [Athena q2_evaluation_time](team_050752609589/screenshots/athena_q2_evaluation_HI-Large.png) | `team_…/s3/analytics/q2_evaluation_time/` |
| Parquet scans **247×** less data than CSV (full scan, HI-Large); **10,091×** for one day | [CSV query](team_050752609589/screenshots/athena_benchmark_csv_HI-Large.png), [Parquet query](team_050752609589/screenshots/athena_benchmark_parquet_HI-Large.png), [dashboard](team_050752609589/screenshots/dashboard_cost_performance.png) | `team_…/s3/analytics/run_log/` |
| Tuning: BQ2 EMR step **40 min 10 s → 2 min 56 s** (Learner Lab), **2 min 48 s** repeated in the team account; ranking step 670 s → 16 s | [EMR step, 2 min 48 s](team_050752609589/screenshots/emr_steps_HI-Small_2026-11-04.png), [checkpoint queries](team_050752609589/screenshots/spark_ui_checkpoint_HI-Small.png), [104 skipped stages](team_050752609589/screenshots/spark_ui_stages_HI-Small_2026-11-04.png) | `learner-lab_…/steps_*.json`, `driver_logs/`, `run_info/` |
| Scale: BQ2 on HI-Large ran on 9 executors / 60 cores, 10,876 tasks (0 failed), 1.7 h of task time inside a 3 min 46 s step; 18.8 GiB read, 36.7 GiB shuffled | [Spark executors](team_050752609589/screenshots/spark_ui_executors_HI-Large_2026-11-07.png) | `team_…/steps_j-0643721XTYCK3JC3WMQ.json` |
| Window: only `--window-days` changed; schemes found 142 / **163** / 157 of 370; join step 9.5 / 11.3 / 15.7 s | [experiment steps](learner-lab_465553375598/screenshots/emr_steps_experiments_HI-Small_2026-11-04.jpg), [w1 arguments](learner-lab_465553375598/screenshots/emr_step_w1_arguments.jpg), [w7 arguments](learner-lab_465553375598/screenshots/emr_step_w7_arguments.jpg) | `learner-lab_…/run_info/experiments/` |
| Kafka live layer: 300,000 events in **96 s**, median **64 ms**, 3,071 alerts (8 Nov) | [Live monitor](team_050752609589/screenshots/dashboard_live_monitor.png), [Kafka topic](team_050752609589/screenshots/ssm_kafka_topic.png) | `team_…/ec2_streaming/` |
| Cost: **USD 3.78** on the HI-Large run day, **USD 18.12** for 1–11 Nov (budget USD 250) | [7 Nov](team_050752609589/screenshots/cost_explorer_2026-11-07.png), [November](team_050752609589/screenshots/cost_explorer_nov_month.png) | — |

Not backed by a saved AWS record: the window experiment's precision for 1 and 7 days (8.7 % / 14.1 %), quoted from
that run's evaluation output in `docs/decisions.md`. The 3-day row is backed by the team account's official run.

## Screenshots by pipeline stage

### 1. Storage: S3 data lake
Raw, curated and analytics zones in one bucket; curated HI-Large is Parquet partitioned by `txn_date` (165 days).

![S3 zones](team_050752609589/screenshots/s3_bucket_zones.png)
![Curated partitions](team_050752609589/screenshots/s3_curated_partitions.png)

### 2. Preparation: Glue ETL and Data Catalog
![Glue ETL run](team_050752609589/screenshots/glue_etl_runs.png)
![Glue Data Catalog](team_050752609589/screenshots/glue_catalog_tables.png)

### 3. Orchestration: Step Functions (one click)
Every execution, including the two failed HI-Small attempts on 4 Nov, then the successful HI-Large run.

![Executions](team_050752609589/screenshots/stepfunctions_executions_list.png)
![HI-Large run](team_050752609589/screenshots/stepfunctions_HI-Large_2026-11-07_graph.png)
![State machine](team_050752609589/screenshots/stepfunctions_graph_detail_HI-Large_2026-11-07.png)

### 4. BQ1 analytics: Athena, and CSV vs Parquet
The same query on raw CSV and on curated Parquet: 15.88 GB against 65.97 MB scanned.

![Athena CSV](team_050752609589/screenshots/athena_benchmark_csv_HI-Large.png)
![Athena Parquet](team_050752609589/screenshots/athena_benchmark_parquet_HI-Large.png)

### 5. BQ2 network analytics: transient EMR and Spark
Clusters are created per run and terminated afterwards. The tuned HI-Small step took 2 min 48 s; the Spark UI shows
the checkpointed queries and 104 stages Spark did not have to recompute (output already available).

![EMR clusters](team_050752609589/screenshots/emr_clusters_terminated.png)
![EMR step](team_050752609589/screenshots/emr_steps_HI-Small_2026-11-04.png)
![Spark checkpoints](team_050752609589/screenshots/spark_ui_checkpoint_HI-Small.png)
![Spark stages](team_050752609589/screenshots/spark_ui_stages_HI-Small_2026-11-04.png)

On HI-Large (7 Nov, 1 primary + 8 core m5.2xlarge) the work was spread over 9 executors and 60 cores: 10,876 tasks,
none failed, 1.7 hours of task time finished inside a 3 min 46 s step.

![Spark executors HI-Large](team_050752609589/screenshots/spark_ui_executors_HI-Large_2026-11-07.png)
![BQ2 evaluation](team_050752609589/screenshots/athena_q2_evaluation_HI-Large.png)

### 6. Window experiment (Learner Lab account)
One cluster, three steps that differ only in `--window-days` (1 / 3 / 7); one duplicate w3 step was cancelled and rerun.

![Experiment steps](learner-lab_465553375598/screenshots/emr_steps_experiments_HI-Small_2026-11-04.jpg)
![w1 arguments](learner-lab_465553375598/screenshots/emr_step_w1_arguments.jpg)
![w7 arguments](learner-lab_465553375598/screenshots/emr_step_w7_arguments.jpg)

Schemes found by the two non-default runs (S3 Select over their `q2_pattern_coverage` output; the object path is
not visible in these captures):

![142 of 370](learner-lab_465553375598/screenshots/s3select_pattern_coverage_142_of_370.jpg)
![157 of 370](learner-lab_465553375598/screenshots/s3select_pattern_coverage_157_of_370.jpg)

### 7. Speed layer: Kafka on EC2
Managed through Systems Manager (no SSH); the serving EC2 carries the `Project=aml-g2` tag teardown uses.

![EC2](team_050752609589/screenshots/ec2_instances.png)
![Kafka topic](team_050752609589/screenshots/ssm_kafka_topic.png)

### 8. Visualisation: dashboard on the serving EC2
Browser bookmarks and profile are blurred in the `_redacted` captures.

![BQ1](team_050752609589/screenshots/dashboard_bq1_HI-Large_redacted.png)
![BQ2 money trail](team_050752609589/screenshots/dashboard_bq2_money_trail_redacted.png)
![Live monitor](team_050752609589/screenshots/dashboard_live_monitor.png)
![Cost and performance](team_050752609589/screenshots/dashboard_cost_performance.png)

### 9. Cost
![7 Nov](team_050752609589/screenshots/cost_explorer_2026-11-07.png)
![November](team_050752609589/screenshots/cost_explorer_nov_month.png)

## Raw files

Inside each account folder, files keep the names AWS gives them.

| File / folder | Contents |
|---|---|
| `steps_<cluster>.json`, `cluster_<cluster>.json` | EMR steps (arguments, start, end) and cluster configuration |
| `driver_logs/<cluster>/…/stdout.gz` | Spark driver output, including per-step timings |
| `run_info/` | BQ2 parameters, thresholds and per-step seconds, as the job wrote them |
| `stepfunctions/`, `glue_runs_<job>.json` | Pipeline executions and Glue job runs |
| `s3/analytics/`, `s3/dq_report/`, `s3/manifests/` | Pipeline outputs as written to S3: BQ1 / BQ2 results, data quality, ingestion row count and md5 |
| `s3/analytics/run_log/` | Every Athena query the pipeline ran: bytes scanned, seconds, cost |
| `athena/` | Two counts quoted in `docs/decisions.md` (daily volumes, self-payments), each with its SQL |
| `s3/streaming_alerts/`, `ec2_streaming/` | Kafka alerts per run; the latest run's producer and consumer logs |
| `screenshots/` | The captures above |
