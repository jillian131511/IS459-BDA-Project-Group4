SELECT variant, txn_date, COUNT(*) AS n_txn, SUM(is_laundering) AS n_laundering
  FROM aml.transactions GROUP BY variant, txn_date ORDER BY variant, txn_date
