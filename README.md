# Follow the Money  AML Risk Intelligence on AWS
**IS459 Big Data Architecture  Group G4**

An end-to-end big data pipeline on the IBM Transactions for Anti-Money-Laundering dataset (Kaggle,
[Altman et al., NeurIPS 2023](https://arxiv.org/abs/2306.16424)) that answers two business questions:

| | Business question | Who benefits | Output |
|---|---|---|---|
| **Q1** | Where should the AML monitoring team spend its limited review capacity? | Transaction-monitoring analysts, Head of AML Compliance | Segment risk (lift + Wilson lower bound), capture curve, **alert-budget optimiser**, heatmaps, bank corridors |
| **Q2** | Which accounts are moving the money, and why do we think so? | Financial-crime investigators | Graph motifs (fan-in/out, 2–4-hop cycles, scatter-gather, gather-scatter), **explainable weight-of-evidence scorecard**, top-1,000 watchlist with reasons, money-trail explorer |

![architecture](docs/architecture.png)

## Repository map

| Path | What it is | Runs on |
|---|---|---|
| `infra/launch_ingestion_ec2.sh` → `ingestion/download_to_s3.sh` | Starts a throw-away EC2 that streams Kaggle → S3 raw zone with an md5/row-count manifest and a log, then terminates itself | EC2 |
| `glue/etl_transactions.py` (+ `patterns_parser.py`) | Raw CSV → curated Parquet: explicit schema, DQ report, implied FX → USD, features | AWS Glue (Spark) |
| `athena/q1_*.sql`, `athena/ddl.sql`, `athena/run_athena.py` | Q1 analytics (UNLOAD to Parquet), table DDL with partition projection, CSV-vs-Parquet benchmark, per-query cost log | Athena via Glue Python shell |
| `emr/network_sql.py`, `emr/network_risk.py` | Q2 graph motifs + scorecard + time-based evaluation, written as portable SQL steps; heavy steps checkpointed | Spark on a transient EMR cluster |
| `stepfunctions/aml_pipeline.asl.json` | Orchestration: ETL → crawler → Q1 → (Q2 ∥ benchmark); auto-terminates EMR on failure | Step Functions |
| `streaming/` | Kafka producer, consumer, scoring rules (`monitor_core.py`), EC2 Kafka setup | EC2 |
| `dashboard/app.py` | Streamlit dashboard (Athena mode or local mode) | EC2 / laptop |
| `infra/create_roles.sh`, `infra/setup.sh`, `infra/teardown.sh` | Pipeline IAM role (normal accounts only), one-shot deploy (idempotent), full clean-up | CloudShell |
| `local/` | Synthetic data generator in the exact Kaggle format + local runners (SQLite) | laptop |
| `tests/test_pipeline.py` | End-to-end local test suite (8 tests) | laptop |
| `docs/` | `decisions.md` (every decision made while running on AWS, with evidence), design rationale, rubric checklist, demo + video scripts | – |

## Quick start

### 0  Test locally (no AWS, ~1 minute)
```bash
pip install pandas numpy networkx           # plus streamlit plotly to view the dashboard
python -m unittest tests/test_pipeline.py -v
python local/make_synthetic_data.py --out data/raw --variant SYN-Small
python local/etl_local.py --trans data/raw/SYN-Small_Trans.csv --patterns data/raw/SYN-Small_Patterns.txt --variant SYN-Small
python local/run_local.py --train-until 2022-09-08
python local/replay_local.py --start 2022/09/08
DATA_MODE=local streamlit run dashboard/app.py      # shows SYNTHETIC data, clearly labelled
```

### 1  Deploy to AWS (us-east-1, from AWS CloudShell)
```bash
cp infra/config.env.example infra/config.env      # set a globally unique BUCKET, e.g. is459-g2-aml-<account id>
# Normal AWS account (our team account): create the pipeline role once, then in config.env set
#   PIPELINE_ROLE=aml-pipeline-role  INSTANCE_PROFILE=aml-pipeline-profile
bash infra/create_roles.sh                         # skip in AWS Academy Learner Lab (LabRole already exists)
bash infra/setup.sh                                # bucket + zones, Glue jobs, crawler, Athena tables, state machine
```

### 2  Ingest (a self-terminating EC2 does it; ~20 s for HI-Small, ~10 min for HI-Large)
```bash
export KAGGLE_API_TOKEN=KGAT_...                   # kaggle.com -> Settings -> API -> Create New Token
bash infra/launch_ingestion_ec2.sh HI-Small HI-Medium HI-Large
source infra/config.env && aws s3 cp s3://$BUCKET/raw/_manifests/HI-Small_Trans.json -   # row count + md5
```
> Tip: before touching the real data, upload `data/raw/SYN-Small_*` to `raw/.../variant=SYN-Small/` and run the
> pipeline on it - a cheap smoke test of every AWS stage.

### 3  Run the whole pipeline (one click)
Official HI-Small run: learn on 1-7 Sep, grade on 8-10 Sep, 3-day window (see `docs/decisions.md`).
```bash
aws stepfunctions start-execution --state-machine-arn <arn printed by setup.sh> \
  --input '{"variant":"HI-Small","core_nodes":2,"run_benchmark":true,"train_until":"2022-09-08","test_until":"2022-09-11","window_days":"3"}'
```
`window_days` is a string. Without `train_until` the job runs on all days and `q2_evaluation_time` stays empty.
Experiments (other windows, `--lineage cache` for the before/after timing) run as extra EMR steps with
`--out-prefix experiments/<tag>` so they never overwrite the official results in `analytics/`.

### 4  Live monitor + dashboard (on the Kafka EC2)
```bash
bash streaming/setup_kafka_ec2.sh
python3 streaming/consumer.py --bucket $BUCKET --variant HI-Small &
python3 streaming/producer.py --source s3://$BUCKET/raw/transactions/variant=HI-Small/HI-Small_Trans.csv \
        --start 2022/09/08 --max-events 200000 --speed 3600
DATA_MODE=athena BUCKET=$BUCKET streamlit run dashboard/app.py --server.port 8501
```

### 5  After Week 13
```bash
bash infra/teardown.sh        # deletes bucket, Glue, Step Functions, EMR; then terminate EC2 instances
```
Also delete the pipeline role (`aml-pipeline-role`, `aml-pipeline-profile`) and any IAM access keys made for the project.

## Results so far (HI-Small, official run)

| What | Result |
|---|---|
| Data quality | 5,078,345 rows in; 0 malformed; 9 duplicates; 0 rows without an FX rate (15 currencies incl. Bitcoin) |
| Q1 | Large ACH transfers (10K-100K USD) dominate; top channel 209x the average rate. Reviewing 5 % of transfers catches 75 % of laundering (learning days) |
| Q2, same days | Top 100 accounts: 64 % laundering accounts vs 1.0 % at random; top 1,000: 13.7 % |
| Q2, next days (official, 8-10 Sep) | Top 1,000: 2.4 % vs 0.45 % at random (about 5x) |
| Typologies caught | 163 of 370 laundering attempts (44 %) |
| Tuning | Q2 on EMR 44 min -> about 3.6 min after checkpointing; identical watchlist |
| Why 3 days | 1 -> 3 days nearly doubles same-days precision; 3 -> 7 adds 0.4 points and costs 39 % more path-join time |
| Whole pipeline | about 16 min end to end (Glue 3 min, Athena 1 min, EMR start 5 min, Q2 3.6 min) |

## Design highlights
* **Portable SQL, tested locally.** Q1 SQL runs unchanged in Athena and SQLite; Q2 SQL runs unchanged in Spark SQL and SQLite, so the exact logic deployed was tested on a laptop first.
* **No label leakage, graded on the future.** Scorecard weights are learned on half the accounts (split on the 2nd-to-last character of the account id) and only from the learning days; the official precision is measured on the other half of the accounts, on later days. The streaming demo replays days the batch layer never saw.
* **Tuned with evidence.** Heavy Spark steps are checkpointed, which cut Q2 from 44 to about 3.6 minutes with an identical result; the 3-day window is backed by a 1/3/7-day experiment.
* **Statistically honest ranking.** Q1 ranks segments by the 95 % Wilson lower bound, so tiny lucky segments cannot top the list.
* **Explainable by design.** Every watchlist account carries plain-English reasons; no black-box model.
* **Cost-aware.** Columnar + partitioned storage, partition projection, transient EMR, lifecycle rules, per-query cost log, teardown script.

See `docs/decisions.md` for the decisions made while running on AWS (with evidence), `docs/design.md` for every design decision with its lecture reference and `docs/rubric_checklist.md` for how each
grading criterion is met.
