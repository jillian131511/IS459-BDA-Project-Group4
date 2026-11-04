"""
Q2 on Amazon EMR (Spark) - laundering-network detection + explainable risk scorecard.

Why EMR (not Glue) for this step: multi-hop path joins are iterative, memory-heavy graph work. Spark keeps
intermediate results in memory instead of writing them to disk between steps (Week 6 "Spark vs. Hadoop
MapReduce", slide 28), and EMR gives us control over cluster size and configuration (Week 6 "What is Amazon
EMR?", slide 33). The cluster is TRANSIENT: Step Functions creates it, runs this step, and terminates it.

Usage (EMR step / spark-submit):
  spark-submit --py-files s3://BUCKET/scripts/network_sql.py emr/network_risk.py \
      --bucket BUCKET --variant HI-Small [--window-days 3] [--top-k 1000]
      [--train-until 2022-09-08 --test-until 2022-09-11]     # time-based grading, see docs/decisions.md
      [--out-prefix experiments/w7 --tag w7]                 # experiment runs: keep official results intact

Reads   s3://BUCKET/curated/transactions/variant=<v>/        (Glue ETL output, Parquet)
        s3://BUCKET/curated/patterns/variant=<v>/            (ground truth typologies, optional)
        s3://BUCKET/analytics/q1_segment_risk/variant=<v>/   (Athena Q1 output)
Writes  s3://BUCKET/analytics/q2_<table>/variant=<v>/        (Parquet, queried by Athena + dashboard)
        s3://BUCKET/analytics/serving/variant=<v>/*.csv      (small files for the Kafka live monitor)
"""
import argparse
import json
import math
import time

from pyspark.sql import SparkSession, functions as F

from network_sql import DEFAULT_PARAMS, FUTURE_TRUTH_SQL, OUTPUTS, STEPS, render

CACHE = {"pairs", "account_stats", "dp_nh", "paths2", "account_features", "account_ranked"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--variant", required=True)
    ap.add_argument("--window-days", type=int, default=3)
    ap.add_argument("--top-k", type=int, default=1000)
    ap.add_argument("--train-until", default="none", help="YYYY-MM-DD: learn only from earlier days")
    ap.add_argument("--test-until", default="none",
                    help="YYYY-MM-DD: official grading period is train_until <= day < test_until")
    ap.add_argument("--out-prefix", default="analytics",
                    help="where outputs go; experiments use e.g. experiments/<tag> so official results stay")
    ap.add_argument("--tag", default="official", help="label stored in run_info")
    ap.add_argument("--lineage", choices=["checkpoint", "cache"], default="checkpoint",
                    help="how heavy steps are kept: checkpoint (default, fast) or cache (original, slow)")
    ap.add_argument("--checkpoint-dir", default="hdfs:///tmp/aml-checkpoints")
    a = ap.parse_args()
    base = f"s3://{a.bucket}"
    out_base = f"{base}/{a.out_prefix.strip('/')}"

    spark = (SparkSession.builder.appName(f"aml-network-risk-{a.variant}")
             .config("spark.sql.adaptive.enabled", "true")            # adaptive query execution handles skew
             .config("spark.sql.adaptive.skewJoin.enabled", "true")
             # Many steps reuse cached views; AQE writes the full plan as text for the Spark UI and that text
             # grew past the 2 GB driver heap (OutOfMemoryError on the first EMR run). Cap it.
             .config("spark.sql.maxPlanStringLength", "100000")
             .getOrCreate())
    # Tuning (Week 10: observe -> hypothesise -> change one thing -> measure). Observed: on EMR the ranking step
    # took ~70% of Q2's time while doing little real work. Hypothesis: each step's plan grows to include every
    # earlier step, so Spark spends its time re-planning and matching cached plans. Change: checkpoint heavy steps,
    # which saves the result and cuts the plan back to "read this saved table". Checkpoints go to the cluster's
    # own HDFS (deleted with the transient cluster).
    if a.lineage == "checkpoint":
        spark.sparkContext.setCheckpointDir(a.checkpoint_dir)

    tx_all = spark.read.parquet(f"{base}/curated/transactions/variant={a.variant}")
    tx_all.createOrReplaceTempView("transactions_all")
    tx = tx_all
    if a.train_until.lower() != "none":
        tx = tx.filter(F.col("txn_date") < a.train_until)  # partition pruning on txn_date
        test_until = a.test_until if a.test_until.lower() != "none" else "9999-12-31"
        spark.sql(FUTURE_TRUTH_SQL.format(train_until=a.train_until, test_until=test_until)) \
            .createOrReplaceTempView("future_truth")
    else:   # no time split -> no future labels; evaluation_time comes out empty
        spark.sql("SELECT CAST(NULL AS STRING) AS acct, 0 AS is_bad_official, 0 AS is_bad_all WHERE 1 = 0") \
            .createOrReplaceTempView("future_truth")
    tx.createOrReplaceTempView("transactions")
    spark.read.parquet(f"{base}/analytics/q1_segment_risk/variant={a.variant}").createOrReplaceTempView("q1_segment_risk")
    try:
        spark.read.parquet(f"{base}/curated/patterns/variant={a.variant}").createOrReplaceTempView("patterns")
    except Exception:  # patterns file is optional - create an empty view so the SQL still runs
        spark.sql("SELECT CAST(NULL AS STRING) AS attempt_id, CAST(NULL AS STRING) AS pattern_type, "
                  "CAST(NULL AS STRING) AS src_id, CAST(NULL AS STRING) AS dst_id WHERE 1 = 0"
                  ).createOrReplaceTempView("patterns")

    t_start = time.time()
    params = dict(DEFAULT_PARAMS, window_days=int(a.window_days), top_k=a.top_k)
    timings = {}
    for name, sql in STEPS:
        if name == "dp_nh":
            # EXACT 99.9th percentile (same linear interpolation as pandas in local/run_local.py). approxQuantile
            # gave different thresholds depending on data layout, which changed the watchlist between runs.
            q = spark.sql("SELECT percentile(max_daily_fan_out, 0.999) AS fo, percentile(max_daily_fan_in, 0.999) AS fi, "
                          "percentile(total_degree, 0.999) AS td FROM account_stats").first()
            params.update(fan_out_k=max(5, math.ceil(q.fo)), fan_in_k=max(5, math.ceil(q.fi)),
                          hub_degree=max(50, math.ceil(q.td)))
            print("data-driven thresholds:", {k: params[k] for k in ("fan_out_k", "fan_in_k", "hub_degree")})
        t = time.time()
        df = spark.sql(render(sql, params))
        if name in CACHE:
            if a.lineage == "checkpoint":
                df = df.checkpoint(eager=True)  # materialise AND cut the plan
            else:
                df = df.cache()
                df.count()  # materialise so later steps reuse it from memory
        df.createOrReplaceTempView(name)
        timings[name] = round(time.time() - t, 1)
        print(f"step {name:18s} {timings[name]:7.1f}s")

    for name in OUTPUTS:
        out = spark.table(name)
        # normalise numeric types so the Athena DDL (bigint / double) always matches the Parquet files
        out = out.select([F.col(f.name).cast("bigint").alias(f.name)
                          if f.dataType.typeName() in ("integer", "short", "byte", "long")
                          else (F.col(f.name).cast("double").alias(f.name)
                                if f.dataType.typeName() in ("float", "decimal") else F.col(f.name))
                          for f in out.schema.fields])
        (out.coalesce(4 if name == "watchlist_edges" else 1)
         .write.mode("overwrite").parquet(f"{out_base}/q2_{name}/variant={a.variant}"))

    # small CSV "serving layer" for the streaming monitor (pandas on EC2 can read it without Spark/pyarrow)
    serving = f"{out_base}/serving/variant={a.variant}"
    spark.table("watchlist").select("acct", "risk_rank", "risk_probability", "reasons") \
        .coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{serving}/watchlist")
    spark.table("q1_segment_risk").select("payment_format", "payment_currency", "is_cross_bank",
                                          "is_cross_currency", "amount_band", "lift", "wilson_lb") \
        .coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{serving}/segment_risk")

    spark.read.parquet(f"{base}/curated/fx_rates/variant={a.variant}").select("currency", "units_per_usd") \
        .coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{serving}/fx_rates")

    run_info = {"variant": a.variant, "tag": a.tag, "lineage": a.lineage, "train_until": a.train_until, "test_until": a.test_until,
                "params": params, "step_seconds": timings, "total_seconds": round(time.time() - t_start, 1)}
    spark.createDataFrame([(json.dumps(run_info),)], "run_info string").coalesce(1).write.mode("overwrite") \
        .text(f"{out_base}/q2_run_info/variant={a.variant}")
    print(json.dumps(run_info, indent=2))


if __name__ == "__main__":
    main()
