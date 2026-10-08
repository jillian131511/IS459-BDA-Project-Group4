-- Q1 | When does laundering happen? Rate by hour of day x day of week (1 = Monday)
WITH g AS (
    SELECT day_of_week, txn_hour, COUNT(*) AS n_txn, SUM(is_laundering) AS n_laundering
    FROM {db}.transactions WHERE variant = '{variant}' {date_filter}
    GROUP BY day_of_week, txn_hour
),
tot AS (SELECT CAST(SUM(n_laundering) AS DOUBLE) / SUM(n_txn) AS base_rate FROM g)
SELECT g.day_of_week, g.txn_hour, g.n_txn, g.n_laundering,
       CAST(g.n_laundering AS DOUBLE) / g.n_txn AS laundering_rate,
       (CAST(g.n_laundering AS DOUBLE) / g.n_txn) / tot.base_rate AS lift
FROM g CROSS JOIN tot
