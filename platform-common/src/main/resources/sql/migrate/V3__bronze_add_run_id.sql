-- 迁移 V3：bronze 补 run_id 列（ADD COLUMNS 是纯元数据变更）。
-- 加列前写入的历史行没有归属批次，由 IngestJob.prepareBronze() 删除（run_id IS NULL）。
ALTER TABLE ${bronze_table} ADD COLUMNS (run_id STRING COMMENT '入湖批次运行ID（对应 ops.batch_runs.run_id）');
