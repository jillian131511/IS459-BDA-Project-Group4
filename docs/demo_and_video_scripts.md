# Week 13 demo plan and video scripts

Everything runs in the **team AWS account** (`g2team4`, us-east-1, bucket `is459-g2-aml-050752609589`).
Goat runs the pipeline; each member owns one part, checks it in the console, presents it and answers on it.

## 0 · Who owns what

| Member | Owns | Checks in the console | Video |
|---|---|---|---|
| 1 · Goat | Running the pipeline, cost, teardown, repo | Step Functions executions, Billing → Cost Explorer | – |
| 2 | Ingestion (self-terminating EC2, manifests) + Step Functions | S3 `raw/`, `raw/_manifests/`, EC2 instances, Step Functions graph | **Films Video A** |
| 3 | Glue cleaning, data quality, exchange rates | Glue → ETL jobs → Runs; Athena `aml.dq_report`, `aml.fx_rates` | – |
| 4 | BQ1 in Athena, budget slider, CSV vs Parquet | Athena `aml.q1_segment_risk`; S3 `analytics/run_log/` | – |
| 5 | BQ2 on EMR, grading, tuning + window evidence | EMR → Clusters → Steps (+ Spark history UI); Athena `aml.q2_evaluation_time` | – |
| 6 | Dashboard, Kafka speed layer | Dashboard link; Kafka EC2 | **Films and edits Video B**, uploads both |

Each member logs into the team console with the group login (never with the IAM access key).

## 1 · Live demo (15-minute slot; every member demos one component)

| Time | Presenter | What is on screen |
|---|---|---|
| 0:00-1:30 | Member 1 | Recap: two questions, users, architecture slide |
| 1:30-3:00 | Member 2 | S3 raw zone + an ingestion manifest; Step Functions graph of the finished official HI-Large run (all green) |
| 3:00-4:30 | Member 3 | Glue job run details; curated Parquet day folders; `aml.dq_report` in Athena |
| 4:30-6:30 | Member 4 | Dashboard BQ1 tab: move the budget slider live (the rule-list heading shows how many segments to monitor); heatmaps, then click a cell in the bank-route heatmap to open that bank's routes; CSV vs Parquet bytes scanned |
| 6:30-9:00 | Member 5 | Dashboard BQ2 tab, top to bottom: watchlist (search "cycle", click a row to open its money trail), then click a flagged neighbour to jump to its trail; scroll to "How the watchlist is scored and graded" for the grading chart (official vs same days); the tuning and window tables |
| 9:00-11:00 | Member 6 | Kafka: producer + consumer, Live monitor tab fills with alerts (or the recorded clip) |
| 11:00-12:30 | Member 1 | Cost from Cost Explorer; teardown plan; individual-contributions slide |
| 12:30-15:00 | all | Buffer + Q&A |

Back-up: if a live step stalls, play the recorded clip of that step and narrate.

## 2 · Video A: "Set up the pipeline end-to-end" (6-8 min, narrated) — Member 2, in the team account

The resources already exist, so every command below **updates** them (all scripts are safe to re-run).
**Do not run `teardown.sh` afterwards** — it would delete the team's data and results.

1. GitHub: click through `ingestion/ glue/ athena/ emr/ stepfunctions/ dashboard/ infra/` (30 s).
2. Team console (region **N. Virginia**) → **CloudShell** (`>_` icon). Upload the repo ZIP
   (GitHub → Code → Download ZIP; CloudShell → Actions → Upload file), then
   `unzip -q IS459-BDA-Project-Group4-main.zip && cd IS459-BDA-Project-Group4-main` (45 s).
3. Settings for the team account (copy-paste, then check) (30 s):
   ```bash
   cp infra/config.env.example infra/config.env
   sed -i -e "s/is459-g2-aml-CHANGE-ME/is459-g2-aml-050752609589/" \
          -e "s/^export PIPELINE_ROLE=LabRole.*/export PIPELINE_ROLE=aml-pipeline-role/" \
          -e "s/^export INSTANCE_PROFILE=LabInstanceProfile.*/export INSTANCE_PROFILE=aml-pipeline-profile/" infra/config.env
   grep -E "^export (BUCKET|PIPELINE_ROLE|INSTANCE_PROFILE)" infra/config.env
   ```
   Expected: the three lines with `is459-g2-aml-050752609589`, `aml-pipeline-role`, `aml-pipeline-profile`.
   Say once: *"Our pipeline is already deployed, so the scripts update each resource in place. On a fresh
   account the same commands create them from scratch."*
4. `bash infra/create_roles.sh` → only the last line, `role=aml-pipeline-role instance_profile=aml-pipeline-profile ready`
   (everything already exists) (30 s).
5. `bash infra/setup.sh` → upload lines, 12 lines ending `"state": "SUCCEEDED"`, `{"JobName": "aml-etl-transactions"}`
   and `{"JobName": "aml-athena-runner"}` (updated, not created), an `"updateDate"` for the state machine, then
   `Deployed.` (no `"Location"` line: the bucket already exists). Then show in the console: S3 bucket, Glue → Data
   Catalog → `aml` tables, Glue → ETL jobs, Glue → Crawlers, Step Functions → `aml-pipeline` → Graph view (2 min).
5b. Step Functions → `aml-pipeline` → Graph view → click **Create transient EMR cluster** → its definition
   (instance type and count, auto-termination, tags). Say: *"BQ2's Spark cluster is not kept running. The pipeline
   creates it for each run and deletes it afterwards, so it only costs while it works."* (30 s)
6. `export KAGGLE_API_TOKEN=KGAT_...` (own token) → `bash infra/launch_ingestion_ec2.sh HI-Small` →
   EC2 → Instances: `aml-ingest` Running, then Terminated after ~2 min (2 min, speed up the wait).
7. `source infra/config.env && aws s3 cp s3://$BUCKET/raw/_manifests/HI-Small_Trans.json -` → `"lines":5078346`,
   `"md5":"9667f3a6323e6ad88e699acf42db9157"` — the same fingerprint as every earlier download (1 min).
8. Dashboard + Kafka server. The team's server already exists, so do **not** launch a second one: show
   `infra/launch_serving_ec2.sh` on GitHub and say *"one command starts a small EC2 that runs Kafka and the
   dashboard as services"*. Then EC2 → Instances → `aml-serving` (Running) → its **Security** tab (port 8501 open)
   → open `http://<public IP>:8501` in a new tab and show the sidebar's data source `athena` (1 min).
9. Kafka: Systems Manager → **Run Command** is how we control the server without SSH. Show the topic:
   Run Command → `AWS-RunShellScript` → target `aml-serving` → command
   `/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic aml-transactions`
   → output shows `PartitionCount: 3` (1 min).
10. Close on the architecture diagram: point at each box and name what was just created — the bucket, the Glue
   job and crawler, the Athena tables, the Step Functions pipeline (with EMR inside it), the ingestion EC2 and
   the serving EC2 (30 s).

## 3 · Video B: "Run the pipeline end-to-end" (6-8 min, narrated) — Member 6 records an HI-Large run

**Before the Kafka recording:** deploy the updated `streaming/producer.py` to
`/opt/aml/streaming/producer.py` on the existing serving EC2. The 8 Nov ordering fix is locally tested, not yet
verified on EC2. Step 4 below resets the topic and starts a fresh consumer; this producer-only update does not
require step 1 or a batch rerun. Record a new live clip and fresh alert/latency measurements. The producer first
reads the entire source and sorts the selected interval on temporary disk, so allow preparation time and disk
space before alerts begin. It keeps all selected events and applies the event limit after chronological sorting.
The full batch execution in step 1 is part of the end-to-end video, not a prerequisite for deploying this fix.

1. Step Functions → `aml-pipeline` → **Start execution**, input
   `{"variant":"HI-Large","core_nodes":8,"instance_type":"m5.2xlarge","run_benchmark":true,"train_until":"2022-10-08","test_until":"2022-11-06","window_days":"3"}`;
   narrate each state as it turns green, speed up the waiting (3 min). The run takes about 24 min and
   costs about USD 2; it rewrites the same official results, so the numbers stay the same.
2. Athena: `SELECT * FROM aml.q1_segment_risk WHERE variant='HI-Large' ORDER BY risk_rank LIMIT 10` and
   `SELECT * FROM aml.q2_evaluation_time WHERE variant='HI-Large' ORDER BY period, k` (1 min).
3. Dashboard: BQ1 tab (slider) and BQ2 tab (watchlist, money trail) (2 min).
4. Kafka: Systems Manager → Run Command on `aml-serving`: `bash /opt/aml/streaming/run_live_demo.sh is459-g2-aml-050752609589 HI-Small 2022/09/08`
   → the dashboard **Live monitor** tab fills with alerts (1 min).
5. S3 `analytics/run_log/`: bytes scanned and cost per query (30 s).

Record every attempt; keep the clean one. Upload both videos to YouTube (unlisted) and add the links to the
README and the appendix slide.

## 4 · Likely Q&A and short answers
* **Why EMR and not Glue for BQ2?** Multi-hop self-joins are iterative and memory-heavy; Spark on EMR lets us keep
  intermediate results and size the cluster; it is transient so it costs only while running (W6 s28, s33).
* **Isn't the precision inflated because you used the labels?** WoE weights are learned on one half of the accounts and
  only from the learning days; the official figure is measured on the other half, on later days (8-10 Sep for HI-Small).
  BQ1 segment-risk features use labels from all learning-period accounts, so the account holdout is not fully
  label-independent. Future-day evaluation is the stronger evidence. We also
  found and fixed a split that graded only 3 % of accounts (`docs/decisions.md`).
* **Why 3 days?** Measured: 1 -> 3 days lifts precision by more than half (8.7 % -> 13.7 %) and finds the most laundering attempts; 3 -> 7 adds
  0.4 points and costs 39 % more path-join time.
* **What did tuning change?** Checkpointing the heavy Spark steps: the BQ2 EMR step went from 40 min to under
  3 min, and the ranking step from 670 s to 16 s (Week 10 loop: observe, hypothesise, change, measure). That release
  also fixed two accuracy issues, so the clean one-variable test is local: 593 s -> 40 s, identical watchlist.
* **Why Wilson lower bound?** A segment with 1 laundering case out of 2 has a 50 % raw rate but tells us almost
  nothing; the lower bound ranks by risk we are statistically confident about.
* **How would this run in production?** Daily files land in `raw/`, an EventBridge schedule starts the same state
  machine, and the consumer reloads the serving CSVs after each run.
* **What about longer cycles?** Enumeration cost grows exponentially; we cap at 4 hops and still flag those
  accounts through fan-out, scatter-gather and segment signals; typology coverage shows what we miss.
* **Data is synthetic, so is this realistic?** It was generated by IBM to mimic real laundering typologies and
  ratios (NeurIPS 2023); the pipeline is data-agnostic and would run on a bank's own transaction files.
