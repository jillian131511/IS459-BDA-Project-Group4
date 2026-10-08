"""
AWS Glue ETL job (PySpark) - RAW zone (CSV) -> CURATED zone (partitioned Parquet)

What it does (Week 4 "Storage -> Transformation -> Orchestration", slide 6; Week 5 Pipeline #1, slide 32):
  1. Reads <variant>_Trans.csv with an EXPLICIT schema. The raw header repeats "Account" twice, which makes a
     crawler-inferred schema ambiguous - a veracity issue we fix at read time (Week 3 "Veracity", slide 6).
  2. Cleans: trims, parses timestamps, drops malformed rows and exact duplicates, and writes a data-quality
     report so every dropped row is accounted for (lineage, Week 3 slide 10).
  3. Normalises every amount to USD using exchange rates IMPLIED by the data itself (cross-currency
     transactions carry both the paid and the received amount).
  4. Derives analysis features (hour, weekday, cross-bank, cross-currency, round amount, amount band,
     global account ids).
  5. Writes Snappy Parquet partitioned by variant and txn_date. Columnar + partitioned = Athena scans fewer
     bytes, so queries are faster and cheaper (Week 2 slides 70-73; Week 5 "AWS Athena", slide 36).
  6. Parses <variant>_Patterns.txt (ground-truth laundering attempts) into a curated table used to evaluate Q2.

Job parameters (Glue "--key value"):
  --RAW_TRANS_PATH     s3://bucket/raw/transactions/variant=HI-Small/HI-Small_Trans.csv
  --RAW_PATTERNS_PATH  s3://bucket/raw/patterns/variant=HI-Small/HI-Small_Patterns.txt   (optional, "none" to skip)
  --CURATED_PATH       s3://bucket/curated
  --VARIANT            HI-Small

Runs on Glue 4.0/5.0. Also runs on plain Spark (e.g. EMR or a laptop) with the same arguments:
  spark-submit glue/etl_transactions.py --RAW_TRANS_PATH ... --RAW_PATTERNS_PATH ... --CURATED_PATH ... --VARIANT ...
"""
import sys

from patterns_parser import parse_patterns_text  # shipped with --extra-py-files
from pyspark.sql import Row, SparkSession, functions as F, types as T

ARGS = ["RAW_TRANS_PATH", "RAW_PATTERNS_PATH", "CURATED_PATH", "VARIANT"]

RAW_SCHEMA = T.StructType([
    T.StructField("ts_raw", T.StringType()),
    T.StructField("from_bank", T.StringType()),
    T.StructField("from_account", T.StringType()),
    T.StructField("to_bank", T.StringType()),
    T.StructField("to_account", T.StringType()),
    T.StructField("amount_received", T.DoubleType()),
    T.StructField("receiving_currency", T.StringType()),
    T.StructField("amount_paid", T.DoubleType()),
    T.StructField("payment_currency", T.StringType()),
    T.StructField("payment_format", T.StringType()),
    T.StructField("is_laundering", T.IntegerType()),
])

AMOUNT_BANDS = [(100, "a <100"), (1_000, "b 100-1K"), (10_000, "c 1K-10K"),
                (100_000, "d 10K-100K"), (1_000_000, "e 100K-1M")]


def get_args():
    """Glue passes --KEY value pairs; fall back to argparse-style parsing when not on Glue."""
    try:
        from awsglue.utils import getResolvedOptions  # type: ignore
        return getResolvedOptions(sys.argv, ARGS)
    except ImportError:
        out, argv = {}, sys.argv[1:]
        for i, tok in enumerate(argv):
            if tok.startswith("--") and tok[2:] in ARGS:
                out[tok[2:]] = argv[i + 1]
        missing = [k for k in ARGS if k not in out]
        if missing:
            raise SystemExit(f"missing args: {missing}")
        return out


def get_spark():
    try:
        from awsglue.context import GlueContext  # type: ignore
        from pyspark.context import SparkContext
        glue_ctx = GlueContext(SparkContext.getOrCreate())
        return glue_ctx.spark_session
    except ImportError:
        return SparkSession.builder.appName("aml-etl").getOrCreate()


def normalise_bank(col):
    # banks appear as "021174" in some files and "21174" in others -> canonical integer string
    return F.regexp_replace(F.trim(col), r"^0+(?=\d)", "")


def amount_band(col):
    expr = None
    for limit, label in AMOUNT_BANDS:
        expr = F.when(col < limit, label) if expr is None else expr.when(col < limit, label)
    return expr.otherwise("f >1M")


def build_fx_rates(df):
    """Units of currency per 1 USD, implied by cross-currency transactions that involve USD.
    Example: paid 100 USD, received 99 EUR  ->  EUR rate = 0.99.
    Median (percentile_approx 0.5) makes it robust to outliers."""
    paid_usd = (df.filter((F.col("payment_currency") == "US Dollar") & (F.col("receiving_currency") != "US Dollar")
                          & (F.col("amount_paid") > 0))
                .select(F.col("receiving_currency").alias("currency"),
                        (F.col("amount_received") / F.col("amount_paid")).alias("rate")))
    recv_usd = (df.filter((F.col("receiving_currency") == "US Dollar") & (F.col("payment_currency") != "US Dollar")
                          & (F.col("amount_received") > 0))
                .select(F.col("payment_currency").alias("currency"),
                        (F.col("amount_paid") / F.col("amount_received")).alias("rate")))
    rates = (paid_usd.unionByName(recv_usd)
             .groupBy("currency")
             .agg(F.expr("percentile_approx(rate, 0.5)").alias("units_per_usd"),
                  F.count("*").alias("n_observations")))
    usd = df.sparkSession.createDataFrame([("US Dollar", 1.0, 0)], "currency string, units_per_usd double, n_observations long")
    return rates.unionByName(usd)


def transform(df, variant):
    """Returns (curated_df, fx_rates_df, partial_dq). Expensive counts are avoided: the DQ report is
    finished in main() by reading the (much smaller) Parquet output back."""
    from pyspark import StorageLevel
    df = (df
          .withColumn("from_bank", normalise_bank(F.col("from_bank")))
          .withColumn("to_bank", normalise_bank(F.col("to_bank")))
          .withColumn("from_account", F.upper(F.trim("from_account")))
          .withColumn("to_account", F.upper(F.trim("to_account")))
          .withColumn("payment_format", F.trim("payment_format"))
          .withColumn("payment_currency", F.trim("payment_currency"))
          .withColumn("receiving_currency", F.trim("receiving_currency"))
          .withColumn("ts", F.to_timestamp(F.trim("ts_raw"), "yyyy/MM/dd HH:mm")))

    valid = (F.col("ts").isNotNull() & F.col("from_account").isNotNull() & F.col("to_account").isNotNull()
             & F.col("amount_paid").isNotNull() & (F.col("amount_paid") > 0)
             & F.col("is_laundering").isin(0, 1))
    df = df.withColumn("is_valid", F.coalesce(valid, F.lit(False))).persist(StorageLevel.MEMORY_AND_DISK)
    counts = df.agg(F.count("*").alias("n"), F.sum(F.col("is_valid").cast("int")).alias("n_valid")).first()
    df = df.filter("is_valid").drop("is_valid")
    df = df.dropDuplicates([c for c in df.columns if c != "ts_raw"])

    fx = build_fx_rates(df).cache()
    df = (df.join(F.broadcast(fx.select(F.col("currency").alias("payment_currency"), "units_per_usd")),
                  "payment_currency", "left")
          .withColumn("amount_usd", F.round(F.col("amount_paid") / F.col("units_per_usd"), 2))
          .drop("units_per_usd"))

    df = (df
          .withColumn("ts_epoch", F.unix_timestamp("ts"))
          .withColumn("txn_date", F.date_format("ts", "yyyy-MM-dd"))
          .withColumn("txn_hour", F.hour("ts"))
          .withColumn("day_of_week", ((F.dayofweek("ts") + 5) % 7) + 1)  # 1 = Monday ... 7 = Sunday
          .withColumn("src_id", F.concat_ws(":", "from_bank", "from_account"))
          .withColumn("dst_id", F.concat_ws(":", "to_bank", "to_account"))
          .withColumn("is_self", (F.col("src_id") == F.col("dst_id")).cast("int"))
          .withColumn("is_cross_bank", (F.col("from_bank") != F.col("to_bank")).cast("int"))
          .withColumn("is_cross_currency", (F.col("payment_currency") != F.col("receiving_currency")).cast("int"))
          .withColumn("is_round", ((F.col("amount_paid") >= 1000) & (F.col("amount_paid") % 1000 == 0)).cast("int"))
          .withColumn("amount_band", amount_band(F.col("amount_usd")))
          .withColumn("txn_id", F.xxhash64("ts_raw", "src_id", "dst_id", "amount_paid", "payment_format",
                                           "receiving_currency"))
          .withColumn("variant", F.lit(variant))
          .withColumn("ts", F.date_format("ts", "yyyy-MM-dd HH:mm")))

    cols = ["txn_id", "ts", "ts_epoch", "txn_hour", "day_of_week", "from_bank", "from_account", "to_bank",
            "to_account", "src_id", "dst_id", "amount_received", "receiving_currency", "amount_paid",
            "payment_currency", "amount_usd", "amount_band", "payment_format", "is_self", "is_cross_bank",
            "is_cross_currency", "is_round", "is_laundering", "variant", "txn_date"]
    dq = {"variant": variant, "rows_in": int(counts["n"]), "rows_valid": int(counts["n_valid"] or 0)}
    return df.select(*cols), fx, dq


def patterns_df(spark, path, variant):
    files = spark.sparkContext.wholeTextFiles(path)  # one file per variant, tens of MB at most
    rows = files.flatMap(lambda kv: [(a, t, d) + tuple(l.split(",")[:11]) for a, t, d, l in parse_patterns_text(kv[1])])
    cols = ["attempt_no", "pattern_type", "pattern_detail"] + [f.name for f in RAW_SCHEMA.fields]
    p = rows.toDF(cols)
    return (p.withColumn("attempt_id", F.concat_ws("-", F.lit(variant), F.col("attempt_no").cast("string")))
             .withColumn("src_id", F.concat_ws(":", normalise_bank(F.col("from_bank")), F.upper(F.trim("from_account"))))
             .withColumn("dst_id", F.concat_ws(":", normalise_bank(F.col("to_bank")), F.upper(F.trim("to_account"))))
             .withColumn("ts", F.date_format(F.to_timestamp(F.trim("ts_raw"), "yyyy/MM/dd HH:mm"), "yyyy-MM-dd HH:mm"))
             .withColumn("amount_paid", F.col("amount_paid").cast("double"))
             .withColumn("variant", F.lit(variant))
             .select("attempt_id", "pattern_type", "pattern_detail", "ts", "src_id", "dst_id", "amount_paid",
                     "payment_currency", "payment_format", "variant"))


def main():
    args = get_args()
    spark = get_spark()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")  # re-runs replace only this variant
    variant, cur = args["VARIANT"], args["CURATED_PATH"].rstrip("/")

    raw = (spark.read.option("header", "true").option("mode", "PERMISSIVE")
           .schema(RAW_SCHEMA).csv(args["RAW_TRANS_PATH"]))
    df, fx, dq = transform(raw, variant)

    (df.repartition("txn_date")  # ~1 file per day per variant: avoids the small-files problem
       .write.mode("overwrite").partitionBy("variant", "txn_date")
       .option("compression", "snappy").parquet(f"{cur}/transactions"))
    fx.withColumn("variant", F.lit(variant)).coalesce(1).write.mode("overwrite") \
      .partitionBy("variant").parquet(f"{cur}/fx_rates")

    # finish the data-quality report from the written Parquet (cheap: columnar, 2 columns read)
    out = spark.read.parquet(f"{cur}/transactions").filter(F.col("variant") == variant)
    o = out.agg(F.count("*").alias("n"), F.sum(F.col("amount_usd").isNull().cast("int")).alias("no_fx"),
                F.sum("is_laundering").alias("laundering")).first()
    dq.update({"rows_malformed_dropped": dq["rows_in"] - dq["rows_valid"],
               "rows_duplicate_dropped": dq["rows_valid"] - int(o["n"]),
               "rows_without_fx_rate": int(o["no_fx"] or 0), "rows_out": int(o["n"]),
               "laundering_rows": int(o["laundering"] or 0)})
    spark.createDataFrame([Row(**dq)]).coalesce(1).write.mode("overwrite").partitionBy("variant") \
         .parquet(f"{cur}/dq_report")

    if args["RAW_PATTERNS_PATH"].lower() != "none":
        patterns_df(spark, args["RAW_PATTERNS_PATH"], variant).coalesce(1).write.mode("overwrite") \
            .partitionBy("variant").parquet(f"{cur}/patterns")
    print("DQ report:", dq)


if __name__ == "__main__":
    main()
