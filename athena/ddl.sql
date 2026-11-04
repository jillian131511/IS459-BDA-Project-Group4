-- Athena DDL - run once by infra/setup.sh (each statement is executed separately; {bucket} is templated).
-- Curated tables (transactions, patterns, fx_rates, dq_report) are created by the Glue crawler instead
-- (Week 4 Glue lab, slides 53-66). Analytics tables below use PARTITION PROJECTION ('injected' variant), so
-- new variants need no crawler run or MSCK REPAIR - queries just filter WHERE variant = '...'.

CREATE DATABASE IF NOT EXISTS aml;

-- Raw CSV, queried only by the CSV-vs-Parquet benchmark (cost/performance X-factor evidence)
CREATE EXTERNAL TABLE IF NOT EXISTS aml.raw_transactions_csv (
  ts_raw string, from_bank string, from_account string, to_bank string, to_account string,
  amount_received double, receiving_currency string, amount_paid double, payment_currency string,
  payment_format string, is_laundering int)
PARTITIONED BY (variant string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://{bucket}/raw/transactions/'
TBLPROPERTIES ('skip.header.line.count'='1', 'projection.enabled'='true',
  'projection.variant.type'='injected',
  'storage.location.template'='s3://{bucket}/raw/transactions/variant=${variant}/');

CREATE EXTERNAL TABLE IF NOT EXISTS aml.q1_segment_risk (payment_format string, payment_currency string, is_cross_bank int, is_cross_currency int, amount_band string, n_txn bigint, n_laundering bigint, usd_volume double, laundered_usd double, laundering_rate double, lift double, wilson_lb double, risk_rank bigint, cum_txn_share double, cum_laundering_share double)
PARTITIONED BY (variant string)
STORED AS PARQUET
LOCATION 's3://{bucket}/analytics/q1_segment_risk/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.variant.type'='injected',
  'storage.location.template'='s3://{bucket}/analytics/q1_segment_risk/variant=${variant}/');

CREATE EXTERNAL TABLE IF NOT EXISTS aml.q1_format_currency (payment_format string, payment_currency string, n_txn bigint, n_laundering bigint, usd_volume double, laundering_rate double, lift double)
PARTITIONED BY (variant string)
STORED AS PARQUET
LOCATION 's3://{bucket}/analytics/q1_format_currency/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.variant.type'='injected',
  'storage.location.template'='s3://{bucket}/analytics/q1_format_currency/variant=${variant}/');

CREATE EXTERNAL TABLE IF NOT EXISTS aml.q1_time_profile (day_of_week int, txn_hour int, n_txn bigint, n_laundering bigint, laundering_rate double, lift double)
PARTITIONED BY (variant string)
STORED AS PARQUET
LOCATION 's3://{bucket}/analytics/q1_time_profile/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.variant.type'='injected',
  'storage.location.template'='s3://{bucket}/analytics/q1_time_profile/variant=${variant}/');

CREATE EXTERNAL TABLE IF NOT EXISTS aml.q1_bank_corridors (from_bank string, to_bank string, n_txn bigint, n_laundering bigint, usd_volume double, laundered_usd double, laundering_rate double, lift double)
PARTITIONED BY (variant string)
STORED AS PARQUET
LOCATION 's3://{bucket}/analytics/q1_bank_corridors/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.variant.type'='injected',
  'storage.location.template'='s3://{bucket}/analytics/q1_bank_corridors/variant=${variant}/');

CREATE EXTERNAL TABLE IF NOT EXISTS aml.q2_watchlist (acct string, risk_rank bigint, risk_probability double, logit double, network_score double, segment_score double, reasons string, max_daily_fan_out bigint, max_daily_fan_in bigint, n_cycles bigint, max_cycle_len bigint, sg_as_source bigint, sg_as_sink bigint, sg_as_mule bigint, gather_scatter_day bigint, seg_lift_avg double, total_degree bigint, is_hub bigint, split string, is_bad bigint)
PARTITIONED BY (variant string)
STORED AS PARQUET
LOCATION 's3://{bucket}/analytics/q2_watchlist/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.variant.type'='injected',
  'storage.location.template'='s3://{bucket}/analytics/q2_watchlist/variant=${variant}/');

CREATE EXTERNAL TABLE IF NOT EXISTS aml.q2_watchlist_edges (src_id string, dst_id string, n_txn bigint, usd double, first_ts bigint, last_ts bigint, any_laundering bigint)
PARTITIONED BY (variant string)
STORED AS PARQUET
LOCATION 's3://{bucket}/analytics/q2_watchlist_edges/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.variant.type'='injected',
  'storage.location.template'='s3://{bucket}/analytics/q2_watchlist_edges/variant=${variant}/');

CREATE EXTERNAL TABLE IF NOT EXISTS aml.q2_evaluation (k bigint, precision_combined double, precision_network_only double, precision_segment_only double, base_rate double, recall_combined double, n_holdout_accounts bigint, n_holdout_bad bigint)
PARTITIONED BY (variant string)
STORED AS PARQUET
LOCATION 's3://{bucket}/analytics/q2_evaluation/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.variant.type'='injected',
  'storage.location.template'='s3://{bucket}/analytics/q2_evaluation/variant=${variant}/');

CREATE EXTERNAL TABLE IF NOT EXISTS aml.q2_evaluation_time (period string, k bigint, precision_combined double, precision_network_only double, precision_segment_only double, base_rate double, recall_combined double, n_holdout_accounts bigint, n_holdout_bad bigint)
PARTITIONED BY (variant string)
STORED AS PARQUET
LOCATION 's3://{bucket}/analytics/q2_evaluation_time/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.variant.type'='injected',
  'storage.location.template'='s3://{bucket}/analytics/q2_evaluation_time/variant=${variant}/');

CREATE EXTERNAL TABLE IF NOT EXISTS aml.q2_signal_precision (signal string, value bigint, woe double, n_cal bigint, n_cal_bad bigint, precision_calibration double)
PARTITIONED BY (variant string)
STORED AS PARQUET
LOCATION 's3://{bucket}/analytics/q2_signal_precision/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.variant.type'='injected',
  'storage.location.template'='s3://{bucket}/analytics/q2_signal_precision/variant=${variant}/');

CREATE EXTERNAL TABLE IF NOT EXISTS aml.q2_pattern_coverage (pattern_type string, n_attempts bigint, n_detected bigint, detection_rate double)
PARTITIONED BY (variant string)
STORED AS PARQUET
LOCATION 's3://{bucket}/analytics/q2_pattern_coverage/'
TBLPROPERTIES ('projection.enabled'='true', 'projection.variant.type'='injected',
  'storage.location.template'='s3://{bucket}/analytics/q2_pattern_coverage/variant=${variant}/');
