-- 批作业运行态（共享状态库，与 Iceberg 的 ops.batch_runs 归档表分工不同）。
-- owner / heartbeat_ms 用于失联对账；handle 是幂等/调度引入的新列，老库由 32_batch_runs_add_handle.sql 补。
CREATE TABLE IF NOT EXISTS lake_batch.batch_runs (
  run_id VARCHAR(80) PRIMARY KEY,
  job VARCHAR(64) NOT NULL,
  command VARCHAR(1000),
  status VARCHAR(16) NOT NULL,
  exit_code INTEGER,
  started_ms BIGINT NOT NULL,
  finished_ms BIGINT,
  duration_ms BIGINT,
  log_file VARCHAR(1000),
  message VARCHAR(1000),
  owner VARCHAR(80) NOT NULL,
  heartbeat_ms BIGINT NOT NULL,
  updated_ms BIGINT NOT NULL,
  handle VARCHAR(255)
);
