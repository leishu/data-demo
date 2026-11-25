-- Spark SQL / Iceberg —— ops：数据质量检查结果。
-- 一行一次检查；run_id 与 ops.batch_runs 对齐，页面按它跳回作业详情。
-- observed 一律是「越大越差」的实测值，observed > threshold 即失败；ERROR 会让作业 FAILED。
-- check 是 SQL 关键字，始终用反引号包住（Trino 侧读的时候用双引号）。
-- 不分区：量级是「检查项数 × 运行次数」，很小。
CREATE TABLE IF NOT EXISTS ${catalog}.${ops_schema}.dq_results (
  run_id       STRING   COMMENT '质量检查所属 run_id，与 ops.batch_runs 对齐',
  snapshot_id  STRING   COMMENT '被检查来源表的最近快照 ID',
  `check`      STRING   COMMENT '检查项：silver_unique / silver_not_null / ...',
  severity     STRING   COMMENT 'ERROR 会让作业失败；WARN 只记录',
  observed     DOUBLE   COMMENT '实测值（越大越差）',
  threshold    DOUBLE   COMMENT '允许上限；observed > threshold 即失败',
  detail       STRING   COMMENT '证据：重复订单号 / 异常行示例',
  checked_ms   BIGINT   COMMENT '检查时刻 epoch 毫秒'
)
USING iceberg
TBLPROPERTIES (
${table_properties}
);
