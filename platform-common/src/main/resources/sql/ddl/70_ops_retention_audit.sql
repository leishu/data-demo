-- Spark SQL / Iceberg —— ops：保留作业的执行前后审计事件。
-- dry-run 也记事件，方便审计「谁在什么时候评估过哪些分区」。
CREATE TABLE IF NOT EXISTS ${catalog}.${ops_schema}.retention_audit (
  run_id       STRING   COMMENT '保留作业运行 ID',
  event        STRING   COMMENT 'retention.start / retention.done',
  table_name   STRING   COMMENT '评估的表名（汇总事件为 *）',
  dry_run      BOOLEAN  COMMENT 'true 表示只列出待删分区，未真正删除',
  partitions   INT      COMMENT '命中的分区数',
  row_count    BIGINT   COMMENT '命中分区的行数',
  audited_ms   BIGINT   COMMENT '审计事件时刻 epoch 毫秒',
  detail       STRING   COMMENT 'dry-run 的分区清单摘要 / 执行结果说明'
)
USING iceberg
TBLPROPERTIES (
${table_properties}
);
