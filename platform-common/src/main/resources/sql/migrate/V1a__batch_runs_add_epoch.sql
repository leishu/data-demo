-- 迁移 V1a：ops.batch_runs 补 epoch 时间列（ADD COLUMNS 是纯元数据操作）。
-- ${columns} 由 InitJob 按实际缺失的列注入；两列都在时本语句不执行。
ALTER TABLE ${batch_runs_table} ADD COLUMNS (${columns});
