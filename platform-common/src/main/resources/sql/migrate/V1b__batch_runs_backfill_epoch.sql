-- 迁移 V1b：按 UTC 墙钟回填旧行的 epoch 列。会话时区在 PlatformConfig 里钉死为 UTC，解释无歧义。
-- 只更新仍为 NULL 的行，可重复执行。
UPDATE ${batch_runs_table}
SET started_ms = unix_millis(started_at), finished_ms = unix_millis(finished_at)
WHERE started_ms IS NULL OR finished_ms IS NULL;
