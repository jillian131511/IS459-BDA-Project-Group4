#!/usr/bin/env bash
# =====================================================================================================
# INGESTION: Kaggle -> S3 raw zone (batch ingestion, Week 2 slides 79-81 / Week 3 slide 34)
#
# Run on a small EC2 instance (t3.medium, Amazon Linux 2023, 40 GB gp3 disk, instance profile = LabInstanceProfile)
# so the 16 GB HI-Large file never travels through a laptop. Files are streamed straight from the
# unzipped archive into S3 - no second copy on disk.
#
#   export BUCKET=is459-g2-aml-<yourname>
#   export KAGGLE_USERNAME=... KAGGLE_KEY=...          # from kaggle.com -> Settings -> API -> Create New Token
#   bash ingestion/download_to_s3.sh HI-Small          # dev
#   bash ingestion/download_to_s3.sh HI-Large          # final demo (allow ~20-30 min)
#
# Result:
#   s3://$BUCKET/raw/transactions/variant=<V>/<V>_Trans.csv
#   s3://$BUCKET/raw/patterns/variant=<V>/<V>_Patterns.txt
#   s3://$BUCKET/raw/_manifests/<V>.json         (row counts + md5 + timestamp = lineage record)
# =====================================================================================================
set -euo pipefail
VARIANT="${1:?usage: download_to_s3.sh <HI-Small|HI-Medium|HI-Large|LI-Small|LI-Medium|LI-Large>}"
: "${BUCKET:?set BUCKET}"
DATASET="ealtman2019/ibm-transactions-for-anti-money-laundering-aml"
WORK="${WORK_DIR:-$HOME/aml_ingest}"
mkdir -p "$WORK" && cd "$WORK"

command -v kaggle >/dev/null || pip3 install --user --quiet kaggle
export PATH="$HOME/.local/bin:$PATH"

for KIND in Trans Patterns; do
  FILE="${VARIANT}_${KIND}.$([ "$KIND" = Trans ] && echo csv || echo txt)"
  DEST="raw/$([ "$KIND" = Trans ] && echo transactions || echo patterns)/variant=${VARIANT}/${FILE}"
  echo ">> downloading $FILE"
  kaggle datasets download -d "$DATASET" -f "$FILE" -p "$WORK" --force
  if [ -f "$FILE.zip" ]; then
    # single pass: decompress once, count + checksum + upload simultaneously
    unzip -p "$FILE.zip" | tee >(wc -l > .rows) >(md5sum | cut -d' ' -f1 > .md5) | aws s3 cp - "s3://$BUCKET/$DEST"
    sleep 2; ROWS=$(tr -d ' ' < .rows); MD5=$(cat .md5)
    rm -f "$FILE.zip" .rows .md5
  else
    ROWS=$(wc -l < "$FILE"); MD5=$(md5sum "$FILE" | cut -d' ' -f1)
    aws s3 cp "$FILE" "s3://$BUCKET/$DEST"
    rm -f "$FILE"
  fi
  echo ">> s3://$BUCKET/$DEST  ($ROWS lines, md5 $MD5)"
  echo "{\"variant\":\"$VARIANT\",\"file\":\"$FILE\",\"s3\":\"s3://$BUCKET/$DEST\",\"lines\":$ROWS,\"md5\":\"$MD5\",\"ingested_at\":\"$(date -u +%FT%TZ)\"}" \
    | aws s3 cp - "s3://$BUCKET/raw/_manifests/${VARIANT}_${KIND}.json"
done
echo "done. Next: start the Step Functions pipeline with {\"variant\": \"$VARIANT\"}"
