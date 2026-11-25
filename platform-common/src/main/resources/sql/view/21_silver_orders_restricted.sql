-- Trino SQL：普通 viewer 的 DWD 明细入口，merchant_id 也哈希；列顺序与 analyst 视图一致。
CREATE OR REPLACE VIEW ${catalog}.${database}.silver_orders_restricted AS
SELECT
    substr(to_hex(sha256(to_utf8(user_id))), 1, 16) AS user_id,
    concat(substr(order_id, 1, 6), '***') AS order_id,
    substr(to_hex(sha256(to_utf8(merchant_id))), 1, 12) AS merchant_id,
    category,
    cast(floor(amount / 100) * 100 AS decimal(18,2)) AS amount,
    currency,
    status,
    is_refund,
    dt,
    event_time,
    updated_at
FROM ${catalog}.${database}.silver_orders;
