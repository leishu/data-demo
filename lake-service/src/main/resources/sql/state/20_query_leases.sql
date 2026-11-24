-- 查询并发名额：带过期时间的租约行，实例被 kill 后由下一次请求回收。
CREATE TABLE IF NOT EXISTS lake_batch.query_leases (
  permit_id VARCHAR(64) PRIMARY KEY,
  client VARCHAR(80) NOT NULL,
  acquired_ms BIGINT NOT NULL,
  expires_ms BIGINT NOT NULL
);
