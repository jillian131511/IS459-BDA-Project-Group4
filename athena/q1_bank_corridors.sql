-- Q1 | Which bank-to-bank corridors carry the most laundered value? (min. 50 transactions per corridor)
WITH g AS (
    SELECT from_bank, to_bank, COUNT(*) AS n_txn, SUM(is_laundering) AS n_laundering,
           SUM(amount_usd) AS usd_volume,
           SUM(CASE WHEN is_laundering = 1 THEN amount_usd ELSE 0 END) AS laundered_usd
    FROM {db}.transactions WHERE variant = '{variant}' {date_filter} AND is_cross_bank = 1
    GROUP BY from_bank, to_bank
    HAVING COUNT(*) >= 50
),
tot AS (SELECT CAST(SUM(is_laundering) AS DOUBLE) / COUNT(*) AS base_rate
        FROM {db}.transactions WHERE variant = '{variant}' {date_filter})
SELECT g.from_bank, g.to_bank, g.n_txn, g.n_laundering, g.usd_volume, g.laundered_usd,
       CAST(g.n_laundering AS DOUBLE) / g.n_txn AS laundering_rate,
       (CAST(g.n_laundering AS DOUBLE) / g.n_txn) / tot.base_rate AS lift
FROM g CROSS JOIN tot
WHERE g.n_laundering > 0
