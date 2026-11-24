-- 迁移 V2a：silver 加 channel 列。Iceberg 加列是纯元数据操作，不改写历史数据文件；
-- 老快照仍只有旧列，Spark/Trino 读新快照时缺值列自动返回 NULL。
ALTER TABLE ${silver_table} ADD COLUMNS (channel STRING COMMENT '下单渠道');
