-- 行数波动：相邻两天 gold 行数比值超过 2 倍视为异常（WARN，不阻断作业）。
-- 只有一天数据时 pairs 为空，observed = 0，检查自然通过。
WITH daily AS (
  SELECT dt, count(*) AS row_cnt FROM ${gold_table} GROUP BY dt
), pairs AS (
  SELECT a.dt, a.row_cnt AS cur_cnt, b.row_cnt AS prev_cnt
  FROM daily a JOIN daily b ON b.dt = date_sub(a.dt, 1)
)
SELECT CAST(coalesce(max(greatest(
         CAST(cur_cnt AS DOUBLE) / nullif(prev_cnt, 0),
         CAST(prev_cnt AS DOUBLE) / nullif(cur_cnt, 0))), 0.0) AS DOUBLE) AS observed,
2.0 AS threshold,
'' AS detail
FROM pairs
