-- 运行历史按开始时间倒序查；与主建表一起失败时由外层 catch 降级（不阻止启动）。
CREATE INDEX IF NOT EXISTS idx_lake_batch_runs_started ON lake_batch.batch_runs(started_ms DESC);
