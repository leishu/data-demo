-- 非空：主键、用户、金额、事件时间不能为空。
SELECT CAST((
  SELECT count(*) FROM ${silver_table}
  WHERE order_id IS NULL OR user_id IS NULL OR amount IS NULL OR event_time IS NULL
) AS DOUBLE) AS observed,
0.0 AS threshold,
coalesce((
  SELECT concat_ws(', ', collect_set(coalesce(order_id, '<NULL>'))) FROM (
    SELECT order_id FROM ${silver_table}
    WHERE order_id IS NULL OR user_id IS NULL OR amount IS NULL OR event_time IS NULL LIMIT 5
  ) d
), '') AS detail
