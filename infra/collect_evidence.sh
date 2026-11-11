#!/usr/bin/env bash
# Saves the AWS run records behind the numbers we quote (pipeline times, EMR step times, BQ2 run_info) into
# raw_aws_output/<account id>/, so the evidence survives infra/teardown.sh. Read-only: it changes nothing in AWS.
# Run it once per AWS account that ran the pipeline, BEFORE teardown (teardown deletes the bucket, the state
# machine's execution history and the Glue job run history). EMR keeps terminated clusters for about 2 months.
#
#   bash infra/collect_evidence.sh [created-after date, default 2026-10-01]
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/config.env
export AWS_PAGER=""
SINCE="${1:-2026-10-01}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
OUT="raw_aws_output/$ACCOUNT_ID"
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

du -sh "$OUT"
echo "done. Check the folder, then commit it."
