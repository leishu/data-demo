-- Spark SQL / Iceberg —— ops：写入级血缘，每次数据写入作业后记一行 run_id -> table -> snapshot_id。
-- 靠 Iceberg 快照映射拿到版本级血缘，不改动任何业务表结构。
-- snapshot_id 取写入后该表元数据里 committed_at 最新的一条（Iceberg 1.11 的 snapshot_id 是 UUID 随机数，不能用 max）；
-- 表刚建好还没写数据时跳过。
CREATE TABLE IF NOT EXISTS ${catalog}.${ops_schema}.lineage_runs (
  run_id       STRING   COMMENT '写入所属运行 ID，与 ops.batch_runs 对齐',
  job          STRING   COMMENT '写表的作业名：ingest / transform / ...',
  table_name   STRING   COMMENT '被写入的业务表名',
  snapshot_id  STRING   COMMENT '写入后的 Iceberg 快照 ID，可据此做时间旅行',
  recorded_ms  BIGINT   COMMENT '记录时刻 epoch 毫秒'
)
USING iceberg
TBLPROPERTIES (
${table_properties}
);
