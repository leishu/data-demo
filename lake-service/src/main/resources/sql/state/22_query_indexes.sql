-- 配额表索引，只是优化：老版本 H2 不支持 IF NOT EXISTS 时，调用方单独吞掉异常即可。
CREATE INDEX IF NOT EXISTS idx_query_events_at ON lake_batch.query_events(at_ms);
CREATE INDEX IF NOT EXISTS idx_query_leases_expires ON lake_batch.query_leases(expires_ms);
