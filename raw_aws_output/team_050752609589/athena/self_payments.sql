SELECT variant, COUNT(*) AS n_txn,
  SUM(CASE WHEN src_id = dst_id THEN 1 ELSE 0 END) AS n_self,
  SUM(CASE WHEN src_id = dst_id AND is_laundering = 1 THEN 1 ELSE 0 END) AS n_self_laundering
  FROM aml.transactions GROUP BY variant
