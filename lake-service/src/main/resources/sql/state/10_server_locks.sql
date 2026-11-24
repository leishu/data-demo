-- 跨实例行锁表：每行一个主题，SELECT ... FOR UPDATE 拿到即持锁。
CREATE TABLE IF NOT EXISTS lake_batch.server_locks (
  name VARCHAR(64) PRIMARY KEY
);
