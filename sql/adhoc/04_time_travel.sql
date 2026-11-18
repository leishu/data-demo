-- 时间旅行：Iceberg 每次提交生成一个快照，Presto 可按快照 ID 或时间点读历史版本
-- 注意：全部为 Presto 专有语法，Spark 侧对应写法见 README（VERSION AS OF / TIMESTAMP AS OF）

-- 1) 看快照列表（$snapshots 是 Iceberg 元数据表，表名含 $ 需要加引号）
-- @presto-only
SELECT snapshot_id,
       committed_at,
       operation,
       summary['total-records'] AS total_records,
       summary['added-records'] AS added_records
FROM iceberg.lake."silver_orders$snapshots"
ORDER BY committed_at DESC
LIMIT 5;

-- 2) 按快照读历史版本：把上一步最新的 snapshot_id 传进来
--    bin/presto-query.sh --sql-file sql/adhoc/04_time_travel.sql --params snapshot_id=1234567890
-- @presto-only
SELECT count(*) AS rows_at_snapshot
FROM iceberg.lake.silver_orders FOR VERSION AS OF ${snapshot_id};

-- 3) 按时间点读（要求该时间点之前已有快照，否则 Presto 会报错）
-- @presto-only
SELECT count(*) AS rows_1h_ago
FROM iceberg.lake.silver_orders FOR TIMESTAMP AS OF current_timestamp - INTERVAL '1' HOUR;
