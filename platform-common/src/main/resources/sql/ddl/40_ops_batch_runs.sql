-- Spark SQL / Iceberg —— ops：批作业运行历史，只落终态（SUCCEEDED / FAILED），不写 RUNNING。
-- 进程被 kill 或超时强杀时没人收尾，留 RUNNING 行会变成永久脏数据。
-- 不分区：单表行数就是「跑过多少次作业」，量级极小。
-- 时间是权威 epoch 毫秒；旧的 started_at / finished_at 墙钟已废弃，读取侧不再依赖。
-- 老表由 InitJob 执行 V1a / V1b 迁移自动 ADD COLUMNS 并按 UTC 墙钟回填。
CREATE TABLE IF NOT EXISTS ${catalog}.${ops_schema}.batch_runs (
  run_id       STRING      COMMENT '运行 ID（服务端生成，与内存记录对齐）',
  job          STRING      COMMENT '作业名：init/ingest/.../all',
  command      STRING      COMMENT '本次进程收到的参数',
  status       STRING      COMMENT 'SUCCEEDED/FAILED',
  exit_code    INT         COMMENT '作业语义退出码：0 成功 1 失败',
  started_at   TIMESTAMP   COMMENT '[已废弃] 开始墙钟，仅展示；请用 started_ms',
  finished_at  TIMESTAMP   COMMENT '[已废弃] 结束墙钟，仅展示；请用 finished_ms',
  duration_ms  BIGINT      COMMENT '耗时(毫秒)',
  log_file     STRING      COMMENT '服务端日志文件，重启后仍可 tail',
  message      STRING      COMMENT '结果说明',
  started_ms   BIGINT      COMMENT '权威开始时间：epoch 毫秒（UTC 绝对时刻）',
  finished_ms  BIGINT      COMMENT '权威结束时间：epoch 毫秒（UTC 绝对时刻）'
)
USING iceberg
TBLPROPERTIES (
${table_properties}
);
