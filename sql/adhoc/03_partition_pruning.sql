-- 分区裁剪验证：下面两条 EXPLAIN 对比，看 Iceberg 有没有把 dt 条件下推成分区过滤
-- 说明：执行计划里出现 constraint on [dt] / partition predicate 即说明只扫指定分区

-- 1) 只查一天
EXPLAIN
SELECT COUNT(*)
FROM iceberg.lake.gold_daily_revenue
WHERE dt = CURRENT_DATE - INTERVAL '1' DAY;

-- 2) 全表扫描（对照）
EXPLAIN
SELECT COUNT(*)
FROM iceberg.lake.gold_daily_revenue;
