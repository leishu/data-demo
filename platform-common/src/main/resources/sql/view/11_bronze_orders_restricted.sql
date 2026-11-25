-- Trino SQL：普通 viewer 的 bronze 明细入口，在 analyst 视图基础上再把 merchant_id 哈希。
-- 与 *_masked 保持同样的列顺序，服务侧按角色切换视图时调用方无感。
CREATE OR REPLACE VIEW ${catalog}.${database}.bronze_orders_restricted AS
SELECT
    substr(to_hex(sha256(to_utf8(user_id))), 1, 16) AS user_id,
    concat(substr(order_id, 1, 6), '***') AS order_id,
    substr(to_hex(sha256(to_utf8(merchant_id))), 1, 12) AS merchant_id,
    category,
    cast(floor(amount / 100) * 100 AS decimal(18,2)) AS amount,
    currency,
    status,
    event_time,
    ingest_time,
    source_file,
    run_id
FROM ${catalog}.${database}.bronze_orders;
