"""
Q2 on Amazon EMR (Spark) - laundering-network detection + explainable risk scorecard.

Why EMR (not Glue) for this step: multi-hop path joins are iterative, memory-heavy graph work. Spark keeps
intermediate results in memory instead of writing them to disk between steps (Week 6 "Spark vs. Hadoop
MapReduce", slide 28), and EMR gives us control over cluster size and configuration (Week 6 "What is Amazon
EMR?", slide 33). The cluster is TRANSIENT: Step Functions creates it, runs this step, and terminates it.

Usage (EMR step / spark-submit):
  spark-submit --py-files s3://BUCKET/scripts/network_sql.py emr/network_risk.py \
      --bucket BUCKET --variant HI-Small [--window-days 3] [--top-k 1000]

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

from network_sql import DEFAULT_PARAMS, OUTPUTS, STEPS, render

CACHE = {"pairs", "account_stats", "dp_nh", "paths2", "account_features", "account_ranked"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--variant", required=True)
    ap.add_argument("--window-days", type=int, default=3)
    ap.add_argument("--top-k", type=int, default=1000)
    ap.add_argument("--train-until", default="none", help="YYYY-MM-DD: learn only from earlier days")
    a = ap.parse_args()
    base = f"s3://{a.bucket}"

    spark = (SparkSession.builder.appName(f"aml-network-risk-{a.variant}")
             .config("spark.sql.adaptive.enabled", "true")            # adaptive query execution handles skew
             .config("spark.sql.adaptive.skewJoin.enabled", "true")
             .getOrCreate())

    tx = spark.read.parquet(f"{base}/curated/transactions/variant={a.variant}")
    if a.train_until.lower() != "none":
        tx = tx.filter(F.col("txn_date") < a.train_until)  # partition pruning on txn_date
    tx.createOrReplaceTempView("transactions")
    spark.read.parquet(f"{base}/analytics/q1_segment_risk/variant={a.variant}").createOrReplaceTempView("q1_segment_risk")
    try:
        spark.read.parquet(f"{base}/curated/patterns/variant={a.variant}").createOrReplaceTempView("patterns")
    except Exception:  # patterns file is optional - create an empty view so the SQL still runs
        spark.sql("SELECT CAST(NULL AS STRING) AS attempt_id, CAST(NULL AS STRING) AS pattern_type, "
                  "CAST(NULL AS STRING) AS src_id, CAST(NULL AS STRING) AS dst_id WHERE 1 = 0"
                  ).createOrReplaceTempView("patterns")

    params = dict(DEFAULT_PARAMS, window_days=int(a.window_days), top_k=a.top_k)
    timings = {}
    for name, sql in STEPS:
        if name == "dp_nh":
            stats = spark.table("account_stats")
            q = stats.approxQuantile(["max_daily_fan_out", "max_daily_fan_in", "total_degree"], [0.999], 0.0001)
            params.update(fan_out_k=max(5, math.ceil(q[0][0])), fan_in_k=max(5, math.ceil(q[1][0])),
                          hub_degree=max(50, math.ceil(q[2][0])))
            print("data-driven thresholds:", {k: params[k] for k in ("fan_out_k", "fan_in_k", "hub_degree")})
        t = time.time()
        df = spark.sql(render(sql, params))
        if name in CACHE:
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
         .write.mode("overwrite").parquet(f"{base}/analytics/q2_{name}/variant={a.variant}"))

    # small CSV "serving layer" for the streaming monitor (pandas on EC2 can read it without Spark/pyarrow)
    serving = f"{base}/analytics/serving/variant={a.variant}"
    spark.table("watchlist").select("acct", "risk_rank", "risk_probability", "reasons") \
        .coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{serving}/watchlist")
    spark.table("q1_segment_risk").select("payment_format", "payment_currency", "is_cross_bank",
                                          "is_cross_currency", "amount_band", "lift", "wilson_lb") \
        .coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{serving}/segment_risk")

    spark.read.parquet(f"{base}/curated/fx_rates/variant={a.variant}").select("currency", "units_per_usd") \
        .coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{serving}/fx_rates")

    run_info = {"variant": a.variant, "params": params, "step_seconds": timings}
    spark.createDataFrame([(json.dumps(run_info),)], "run_info string").coalesce(1).write.mode("overwrite") \
        .text(f"{base}/analytics/q2_run_info/variant={a.variant}")
    print(json.dumps(run_info, indent=2))


if __name__ == "__main__":
    main()
