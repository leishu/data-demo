-- Trino SQL：分析角色的 DWD 明细入口。脱敏口径与 bronze_orders_masked 完全一致，
-- 额外保留 is_refund / dt / updated_at 供退款与时效分析；gold 聚合值不脱敏。
CREATE OR REPLACE VIEW ${catalog}.${database}.silver_orders_masked AS
SELECT
    substr(to_hex(sha256(to_utf8(user_id))), 1, 16) AS user_id,
    concat(substr(order_id, 1, 6), '***') AS order_id,
    merchant_id,
    category,
    cast(floor(amount / 100) * 100 AS decimal(18,2)) AS amount,
    currency,
    status,
    is_refund,
    dt,
    event_time,
    updated_at
FROM ${catalog}.${database}.silver_orders;
