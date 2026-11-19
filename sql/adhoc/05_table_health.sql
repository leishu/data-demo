-- 表健康度巡检：文件数量 / 小文件 / 分区分布 / 快照增长
-- 全部为 Trino 专有语法（Iceberg 元数据表 "$" 后缀）
-- @trino-only
SELECT count(*)                            AS file_cnt,
       sum(record_count)                   AS total_records,
       round(cast(sum(file_size_in_bytes) AS double) / 1024 / 1024, 2) AS total_mb,
       cast(avg(file_size_in_bytes) AS bigint) AS avg_file_bytes,
       min(file_size_in_bytes)             AS min_file_bytes
FROM iceberg.lake."gold_daily_revenue$files";

-- @trino-only
SELECT partition, record_count, file_count
FROM iceberg.lake."gold_daily_revenue$partitions"
ORDER BY record_count DESC;

-- @trino-only
SELECT snapshot_id, committed_at, operation, element_at(summary, 'total-records') AS total_records
FROM iceberg.lake."gold_daily_revenue$snapshots"
ORDER BY committed_at DESC
LIMIT 10;
