-- Trino SQL（不是 Spark DDL）：分析角色只读这张脱敏视图，基础表只给 etl / auditor。
-- 脱敏口径：user_id 取 SHA-256 前 16 位；order_id 截前 6 位 + ***；amount 向下分桶到 100。
-- merchant_id 在本视图保留明文——它按角色授权，只给 analyst，不给普通 viewer。
-- channel 等 evolve 可选列刻意不进明细视图：分析口径走 gold 聚合表。
CREATE OR REPLACE VIEW ${catalog}.${database}.bronze_orders_masked AS
SELECT
    substr(to_hex(sha256(to_utf8(user_id))), 1, 16) AS user_id,
    concat(substr(order_id, 1, 6), '***') AS order_id,
    merchant_id,
    category,
    cast(floor(amount / 100) * 100 AS decimal(18,2)) AS amount,
    currency,
    status,
    event_time,
    ingest_time,
    source_file,
    run_id
FROM ${catalog}.${database}.bronze_orders;
