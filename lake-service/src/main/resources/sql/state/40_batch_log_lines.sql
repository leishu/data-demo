-- 每个 run 的日志尾部，只留最近 2000 行（MAX_LINES 由 Java 侧裁剪）。
CREATE TABLE IF NOT EXISTS lake_batch.batch_log_lines (
  run_id VARCHAR(80) NOT NULL,
  seq BIGINT NOT NULL,
  line VARCHAR(2000),
  at_ms BIGINT NOT NULL,
  PRIMARY KEY (run_id, seq)
);
