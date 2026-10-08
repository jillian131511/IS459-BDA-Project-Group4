-- Q1 | Where should the AML monitoring team focus its limited alert-review capacity?
-- Risk of each monitoring SEGMENT = payment format x payment currency x cross-bank x cross-currency x amount band.
--   laundering_rate : share of the segment's transactions that are laundering
--   lift            : segment rate / overall rate        (3.0 = three times riskier than average)
--   wilson_lb       : 95% Wilson lower bound of the rate - ranks segments by risk we are statistically sure of,
--                     so a tiny segment with 1 laundering txn out of 2 cannot top the list by luck
--   cum_*_share     : if the team monitors segments in this order, what share of all transactions they must
--                     review vs. what share of laundering they catch  -> the "alert budget" capture curve
-- Portable SQL: runs in Amazon Athena (Trino) and in SQLite for local tests. {db}/{variant} are templated.
WITH seg AS (
    SELECT payment_format, payment_currency, is_cross_bank, is_cross_currency, amount_band,
           COUNT(*)                                                   AS n_txn,
           SUM(is_laundering)                                         AS n_laundering,
           SUM(amount_usd)                                            AS usd_volume,
           SUM(CASE WHEN is_laundering = 1 THEN amount_usd ELSE 0 END) AS laundered_usd
    FROM {db}.transactions
    WHERE variant = '{variant}' {date_filter}
    GROUP BY payment_format, payment_currency, is_cross_bank, is_cross_currency, amount_band
),
tot AS (
    SELECT SUM(n_txn) AS all_txn, SUM(n_laundering) AS all_laundering FROM seg
),
scored AS (
    SELECT s.*,
           CAST(s.n_laundering AS DOUBLE) / s.n_txn AS laundering_rate,
           (CAST(s.n_laundering AS DOUBLE) / s.n_txn) / (CAST(t.all_laundering AS DOUBLE) / t.all_txn) AS lift,
           ( (CAST(s.n_laundering AS DOUBLE) / s.n_txn) + 1.9208 / s.n_txn
             - 1.96 * sqrt( (CAST(s.n_laundering AS DOUBLE) / s.n_txn) * (1 - CAST(s.n_laundering AS DOUBLE) / s.n_txn) / s.n_txn
                            + 0.9604 / (CAST(s.n_txn AS DOUBLE) * s.n_txn) )
           ) / (1 + 3.8416 / s.n_txn) AS wilson_lb,
           t.all_txn, t.all_laundering
    FROM seg s CROSS JOIN tot t
)
SELECT payment_format, payment_currency, is_cross_bank, is_cross_currency, amount_band,
       n_txn, n_laundering, usd_volume, laundered_usd, laundering_rate, lift, wilson_lb,
       ROW_NUMBER() OVER (ORDER BY wilson_lb DESC, n_txn DESC) AS risk_rank,
       CAST(SUM(n_txn) OVER (ORDER BY wilson_lb DESC, n_txn DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DOUBLE)
           / all_txn AS cum_txn_share,
       CAST(SUM(n_laundering) OVER (ORDER BY wilson_lb DESC, n_txn DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DOUBLE)
           / all_laundering AS cum_laundering_share
FROM scored
