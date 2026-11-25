-- 唯一性：DWD 一行一单，重复的 order_id 数量必须为 0。
-- ODS 故意保留 3% 重复单，去重发生在 bronze -> silver；这里检查去重结果。
SELECT CAST((
  SELECT count(*) FROM (
    SELECT order_id FROM ${silver_table} GROUP BY order_id HAVING count(*) > 1
  ) d
) AS DOUBLE) AS observed,
0.0 AS threshold,
coalesce((
  SELECT concat_ws(', ', collect_set(order_id)) FROM (
    SELECT order_id FROM ${silver_table} GROUP BY order_id HAVING count(*) > 1 LIMIT 5
  ) d
), '') AS detail
