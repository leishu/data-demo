-- 净收入一致性：gold 的 net_revenue 必须等于 gmv - refund_amount（允许 1 分舍入误差）。
SELECT CAST(coalesce(max(abs(net_revenue - (gmv - refund_amount))), 0.0) AS DOUBLE) AS observed,
0.01 AS threshold,
coalesce((
  SELECT concat_ws(', ', collect_set(concat(CAST(dt AS STRING), '/', merchant_id, '/', category))) FROM (
    SELECT dt, merchant_id, category FROM ${gold_table}
    WHERE abs(net_revenue - (gmv - refund_amount)) > 0.01 LIMIT 5
  ) d
), '') AS detail
FROM ${gold_table}
