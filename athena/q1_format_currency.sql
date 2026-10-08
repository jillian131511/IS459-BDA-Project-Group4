-- Q1 | Heatmap: laundering lift by payment format x payment currency (dashboard + pitch visual)
WITH g AS (
    SELECT payment_format, payment_currency, COUNT(*) AS n_txn, SUM(is_laundering) AS n_laundering,
           SUM(amount_usd) AS usd_volume
    FROM {db}.transactions WHERE variant = '{variant}' {date_filter}
    GROUP BY payment_format, payment_currency
),
tot AS (SELECT CAST(SUM(n_laundering) AS DOUBLE) / SUM(n_txn) AS base_rate FROM g)
SELECT g.payment_format, g.payment_currency, g.n_txn, g.n_laundering, g.usd_volume,
       CAST(g.n_laundering AS DOUBLE) / g.n_txn AS laundering_rate,
       (CAST(g.n_laundering AS DOUBLE) / g.n_txn) / tot.base_rate AS lift
FROM g CROSS JOIN tot
