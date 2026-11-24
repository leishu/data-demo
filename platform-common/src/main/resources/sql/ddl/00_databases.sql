-- Spark SQL / Iceberg —— 建库（幂等）
-- 建库 DDL 不能带 COMMENT：Hadoop catalog 拒绝非空 namespace metadata。
CREATE DATABASE IF NOT EXISTS ${catalog}.${database};
CREATE DATABASE IF NOT EXISTS ${catalog}.${ops_schema};
