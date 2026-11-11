#!/usr/bin/env bash
# Saves the AWS records behind the numbers we quote (run times, accuracy results, data quality, Athena counts,
# streaming logs) into raw_aws_output/<label>_<account id>/, so the evidence survives infra/teardown.sh. It changes no
# pipeline resource; it only adds two Athena result files and a copy of the serving EC2's logs to the bucket
# (athena-results/, evidence/ec2/), which teardown deletes with the bucket. The Athena counts scan a few columns
# (cents on HI-Large).
# Run it once per AWS account that ran the pipeline, BEFORE teardown (teardown deletes the bucket, the state
# machine's execution history and the Glue job run history). EMR keeps terminated clusters for about 2 months.
#
#   bash infra/collect_evidence.sh <label> [created-after date, default 2026-10-01]
#   label: which account this is, e.g. team or learner-lab  ->  raw_aws_output/team_050752609589/
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/config.env
export AWS_PAGER=""
LABEL="${1:?usage: bash infra/collect_evidence.sh <label: team | learner-lab> [created-after date]}"
SINCE="${2:-2026-10-01}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
OUT="raw_aws_output/${LABEL}_${ACCOUNT_ID}"
mkdir -p "$OUT"
echo "saving to $OUT"

# 1. Step Functions: every pipeline execution, with input (variant) and start/stop times
SM_ARN="arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE}"
if aws stepfunctions describe-state-machine --state-machine-arn "$SM_ARN" >/dev/null 2>&1; then
  mkdir -p "$OUT/stepfunctions"
  for ex in $(aws stepfunctions list-executions --state-machine-arn "$SM_ARN" \
                --query "executions[].executionArn" --output text); do
    aws stepfunctions describe-execution --execution-arn "$ex" \
      --query "{name:name,status:status,start:startDate,stop:stopDate,input:input}" --output json \
      > "$OUT/stepfunctions/${ex##*:}.json"
  done
else
  echo "state machine $STATE_MACHINE not found (already torn down?)"
fi

# 2. Glue: ETL and Athena-runner job runs (ExecutionTime is in seconds)
for job in "$ETL_JOB" "$ATHENA_JOB"; do
  aws glue get-job-runs --job-name "$job" --query \
    "JobRuns[].{Id:Id,State:JobRunState,Start:StartedOn,End:CompletedOn,ExecutionTime:ExecutionTime,Workers:NumberOfWorkers,Args:Arguments}" \
    --output json > "$OUT/glue_runs_$job.json" 2>/dev/null || echo "glue job $job not found"
done

# 3. EMR: every aml-q2* cluster (including terminated ones) with its steps, arguments and times
for id in $(aws emr list-clusters --created-after "$SINCE" \
              --query "Clusters[?starts_with(Name,'aml-q2')].Id" --output text); do
  aws emr describe-cluster --cluster-id "$id" --output json > "$OUT/cluster_$id.json"
  aws emr list-steps --cluster-id "$id" --query \
    "Steps[].{Id:Id,Name:Name,State:Status.State,Start:Status.Timeline.StartDateTime,End:Status.Timeline.EndDateTime,Args:Config.Args}" \
    --output json > "$OUT/steps_$id.json"
  # Spark driver log (cluster deploy mode: the application's first container); holds the per-step timings
  aws s3 cp "s3://$BUCKET/emr-logs/$id/containers/" "$OUT/driver_logs/$id/" --recursive \
    --exclude "*" --include "*_01_000001/stdout.gz" --only-show-errors || true
  echo "EMR $id"
done

# 4. S3: BQ2 run_info (parameters, per-step seconds, total) for the official runs and the experiments
aws s3 cp "s3://$BUCKET/analytics/q2_run_info/" "$OUT/run_info/analytics/" --recursive --only-show-errors || true
aws s3 cp "s3://$BUCKET/experiments/" "$OUT/run_info/experiments/" --recursive \
  --exclude "*" --include "*/q2_run_info/*" --only-show-errors || true

# 5. S3: the results behind the accuracy figures (all small Parquet / JSON)
#    BQ2 evaluation, pattern coverage (schemes found) and signal precision; BQ1 tables; data-quality report;
#    ingestion manifests (row count + md5); Athena cost log; streaming alerts
for t in q2_evaluation q2_evaluation_time q2_pattern_coverage q2_signal_precision \
         q1_segment_risk q1_format_currency q1_time_profile q1_bank_corridors run_log; do
  aws s3 cp "s3://$BUCKET/analytics/$t/" "$OUT/s3/analytics/$t/" --recursive --only-show-errors || true
done
aws s3 cp "s3://$BUCKET/experiments/" "$OUT/s3/experiments/" --recursive --exclude "*" \
  --include "*/q2_evaluation/*" --include "*/q2_evaluation_time/*" --include "*/q2_pattern_coverage/*" \
  --only-show-errors || true
aws s3 cp "s3://$BUCKET/curated/dq_report/" "$OUT/s3/dq_report/" --recursive --only-show-errors || true
aws s3 cp "s3://$BUCKET/analytics/serving/" "$OUT/s3/serving_hubs/" --recursive --exclude "*" --include "*/hubs/*" \
  --only-show-errors || true   # hub accounts the live velocity rule skips
aws s3 cp "s3://$BUCKET/raw/_manifests/" "$OUT/s3/manifests/" --recursive --only-show-errors || true
aws s3 cp "s3://$BUCKET/streaming/alerts/" "$OUT/s3/streaming_alerts/" --recursive --only-show-errors || true
echo "S3 results saved"

# 6. Athena: counts quoted in docs/decisions.md (daily volumes and laundering share, self-payments)
athena_csv() {  # $1 output name, $2 SQL
  local qid state loc
  qid=$(aws athena start-query-execution --work-group primary --query-execution-context Database=aml \
          --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" \
          --query-string "$2" --query QueryExecutionId --output text) || { echo "athena $1 not started"; return 0; }
  while :; do
    state=$(aws athena get-query-execution --query-execution-id "$qid" --query QueryExecution.Status.State --output text)
    case "$state" in QUEUED|RUNNING) sleep 3;; *) break;; esac
  done
  if [ "$state" = "SUCCEEDED" ]; then
    loc=$(aws athena get-query-execution --query-execution-id "$qid" \
            --query QueryExecution.ResultConfiguration.OutputLocation --output text)
    mkdir -p "$OUT/athena"; aws s3 cp "$loc" "$OUT/athena/$1.csv" --only-show-errors
    printf '%s\n' "$2" > "$OUT/athena/$1.sql"; echo "athena $1 saved"
  else
    echo "athena $1 $state: $(aws athena get-query-execution --query-execution-id "$qid" \
           --query QueryExecution.Status.StateChangeReason --output text)"
  fi; }
athena_csv daily_counts "SELECT variant, txn_date, COUNT(*) AS n_txn, SUM(is_laundering) AS n_laundering
  FROM aml.transactions GROUP BY variant, txn_date ORDER BY variant, txn_date"
athena_csv self_payments "SELECT variant, COUNT(*) AS n_txn,
  SUM(CASE WHEN src_id = dst_id THEN 1 ELSE 0 END) AS n_self,
  SUM(CASE WHEN src_id = dst_id AND is_laundering = 1 THEN 1 ELSE 0 END) AS n_self_laundering
  FROM aml.transactions GROUP BY variant"

# 7. Serving EC2 (Kafka): the last live-demo run's logs and stats, fetched through Systems Manager via S3
IID=$(aws ec2 describe-instances --filters Name=tag:Project,Values=aml-g2 Name=instance-state-name,Values=running \
        --query "Reservations[].Instances[?Tags[?Key=='Name' && Value!='aml-ingest']].InstanceId" --output text | head -1)
if [ -n "$IID" ]; then
  CMD="cd /opt/aml/streaming; for f in /var/log/aml_consumer.log /var/log/aml_producer.log out/stats.json out/alerts.jsonl; do [ -f \$f ] && aws s3 cp \$f s3://$BUCKET/evidence/ec2/\$(basename \$f); done; ls -l out /var/log/aml_*"
  cid=$(aws ssm send-command --instance-ids "$IID" --document-name AWS-RunShellScript \
          --parameters "{\"commands\":[\"$CMD\"]}" --query Command.CommandId --output text) || cid=""
  if [ -n "$cid" ]; then
    aws ssm wait command-executed --command-id "$cid" --instance-id "$IID" || true
    aws s3 cp "s3://$BUCKET/evidence/ec2/" "$OUT/ec2_streaming/" --recursive --only-show-errors || true
    echo "EC2 $IID streaming logs saved"
  fi
else
  echo "no running serving EC2 found: start it (aws ec2 start-instances) to save the streaming logs, or skip"
fi

du -sh "$OUT"
echo "done. Check the folder, then commit it."
