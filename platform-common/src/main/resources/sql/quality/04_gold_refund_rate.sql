-- 退款率：单组 (dt, 商户, 品类) 的 refund_amount / gmv 必须在 0~1。
-- gmv = 0 的行跳过（视为 0），避免除零；observed 取最大值，超 1.0 即失败。
SELECT CAST(coalesce(max(CASE WHEN gmv > 0 THEN refund_amount / gmv ELSE 0.0 END), 0.0) AS DOUBLE) AS observed,
1.0 AS threshold,
coalesce((
  SELECT concat_ws(', ', collect_set(concat(CAST(dt AS STRING), '/', merchant_id, '/', category))) FROM (
    SELECT dt, merchant_id, category FROM ${gold_table}
    WHERE gmv > 0 AND refund_amount / gmv > 1 LIMIT 5
  ) d
), '') AS detail
FROM ${gold_table}
