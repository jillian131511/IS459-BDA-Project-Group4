#!/usr/bin/env bash
# =====================================================================================================
# One-shot deployment of the whole pipeline (idempotent - safe to re-run after code changes).
# Works in an AWS Academy Learner Lab account (uses the pre-created LabRole) or any account with
# equivalent roles. Run from the repo root, in AWS CloudShell or on the ingestion EC2 instance:
#
#   cp infra/config.env.example infra/config.env   # edit BUCKET (must be globally unique)
#   bash infra/setup.sh
# =====================================================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
source infra/config.env

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${PIPELINE_ROLE}"
echo "account=$ACCOUNT_ID region=$AWS_REGION bucket=$BUCKET role=$ROLE_ARN"

# ---------- 1. S3 data lake: raw / curated / analytics zones (Week 4 slide 6: landing -> curated) ----------
aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null || {
  if [ "$AWS_REGION" = "us-east-1" ]; then aws s3api create-bucket --bucket "$BUCKET"
  else aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint="$AWS_REGION"; fi; }
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
# lifecycle: Athena query results and EMR logs expire after 7 days (storage cost hygiene)
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" --lifecycle-configuration '{
  "Rules": [
    {"ID": "expire-athena-results", "Status": "Enabled", "Filter": {"Prefix": "athena-results/"}, "Expiration": {"Days": 7}},
    {"ID": "expire-emr-logs", "Status": "Enabled", "Filter": {"Prefix": "emr-logs/"}, "Expiration": {"Days": 7}}]}'

# ---------- 2. Code to S3 ----------
aws s3 cp glue/etl_transactions.py   "s3://$BUCKET/scripts/glue/etl_transactions.py"
aws s3 cp glue/patterns_parser.py    "s3://$BUCKET/scripts/glue/patterns_parser.py"
aws s3 cp athena/run_athena.py       "s3://$BUCKET/scripts/athena/run_athena.py"
aws s3 cp athena/ --recursive --exclude "*" --include "*.sql" "s3://$BUCKET/scripts/athena/"
aws s3 cp emr/network_risk.py        "s3://$BUCKET/scripts/emr/network_risk.py"
aws s3 cp emr/network_sql.py         "s3://$BUCKET/scripts/emr/network_sql.py"

# ---------- 3. Glue Data Catalog database + Athena tables (projection) ----------
python3 athena/run_athena.py --bucket "$BUCKET" --mode ddl --region "$AWS_REGION"

# ---------- 4. Glue crawler over the curated zone ----------
TARGETS="{\"S3Targets\":[{\"Path\":\"s3://$BUCKET/curated/transactions/\"},{\"Path\":\"s3://$BUCKET/curated/patterns/\"},{\"Path\":\"s3://$BUCKET/curated/fx_rates/\"},{\"Path\":\"s3://$BUCKET/curated/dq_report/\"}]}"
if aws glue get-crawler --name "$CRAWLER" >/dev/null 2>&1; then
  aws glue update-crawler --name "$CRAWLER" --role "$ROLE_ARN" --database-name aml --targets "$TARGETS"
else
  aws glue create-crawler --name "$CRAWLER" --role "$ROLE_ARN" --database-name aml --targets "$TARGETS" \
    --schema-change-policy UpdateBehavior=UPDATE_IN_DATABASE,DeleteBehavior=LOG \
    --recrawl-policy RecrawlBehavior=CRAWL_EVERYTHING
fi

# ---------- 5. Glue jobs ----------
ETL_CMD="{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET/scripts/glue/etl_transactions.py\",\"PythonVersion\":\"3\"}"
ETL_ARGS="{\"--extra-py-files\":\"s3://$BUCKET/scripts/glue/patterns_parser.py\",\"--enable-metrics\":\"true\",\"--enable-continuous-cloudwatch-log\":\"true\",\"--job-language\":\"python\"}"
if aws glue get-job --job-name "$ETL_JOB" >/dev/null 2>&1; then
  aws glue update-job --job-name "$ETL_JOB" --job-update "{\"Role\":\"$ROLE_ARN\",\"Command\":$ETL_CMD,\"DefaultArguments\":$ETL_ARGS,\"GlueVersion\":\"4.0\",\"WorkerType\":\"$ETL_WORKER_TYPE\",\"NumberOfWorkers\":$ETL_WORKERS,\"Timeout\":120}"
else
  aws glue create-job --name "$ETL_JOB" --role "$ROLE_ARN" --command "$ETL_CMD" --default-arguments "$ETL_ARGS" \
    --glue-version 4.0 --worker-type "$ETL_WORKER_TYPE" --number-of-workers "$ETL_WORKERS" --timeout 120
fi
ATH_CMD="{\"Name\":\"pythonshell\",\"ScriptLocation\":\"s3://$BUCKET/scripts/athena/run_athena.py\",\"PythonVersion\":\"3.9\"}"
if aws glue get-job --job-name "$ATHENA_JOB" >/dev/null 2>&1; then
  aws glue update-job --job-name "$ATHENA_JOB" --job-update "{\"Role\":\"$ROLE_ARN\",\"Command\":$ATH_CMD,\"MaxCapacity\":0.0625,\"Timeout\":60}"
else
  aws glue create-job --name "$ATHENA_JOB" --role "$ROLE_ARN" --command "$ATH_CMD" --max-capacity 0.0625 --timeout 60
fi

# ---------- 6. Step Functions state machine ----------
sed -e "s|__BUCKET__|$BUCKET|g" -e "s|__ETL_JOB__|$ETL_JOB|g" -e "s|__ATHENA_JOB__|$ATHENA_JOB|g" \
    -e "s|__CRAWLER__|$CRAWLER|g" -e "s|__EMR_RELEASE__|$EMR_RELEASE|g" \
    -e "s|__EMR_SERVICE_ROLE__|$EMR_SERVICE_ROLE|g" -e "s|__EMR_EC2_PROFILE__|$EMR_EC2_PROFILE|g" \
    stepfunctions/aml_pipeline.asl.json > /tmp/aml_pipeline.asl.json
SM_ARN="arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE}"
if aws stepfunctions describe-state-machine --state-machine-arn "$SM_ARN" >/dev/null 2>&1; then
  aws stepfunctions update-state-machine --state-machine-arn "$SM_ARN" --definition file:///tmp/aml_pipeline.asl.json --role-arn "$ROLE_ARN"
else
  aws stepfunctions create-state-machine --name "$STATE_MACHINE" --definition file:///tmp/aml_pipeline.asl.json --role-arn "$ROLE_ARN"
fi

cat <<EOF

Deployed. Next steps:
  1. Ingest data      : bash ingestion/download_to_s3.sh HI-Small
  2. Run the pipeline : aws stepfunctions start-execution --state-machine-arn $SM_ARN \\
                          --input '{"variant":"HI-Small","core_nodes":2,"run_benchmark":true}'
  3. Dashboard        : DATA_MODE=athena BUCKET=$BUCKET streamlit run dashboard/app.py
  4. When finished    : bash infra/teardown.sh   (deletes EVERYTHING - required after Week 13)
EOF
