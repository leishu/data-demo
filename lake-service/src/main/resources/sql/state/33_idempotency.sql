-- 批作业提交幂等键：同一 key 只对应一个 run_id。
CREATE TABLE IF NOT EXISTS lake_batch.idempotency (
  idem_key VARCHAR(128) PRIMARY KEY,
  run_id VARCHAR(80) NOT NULL
);
