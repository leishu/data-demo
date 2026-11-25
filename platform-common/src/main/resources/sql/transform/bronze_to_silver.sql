-- Spark SQL / Iceberg —— ODS -> DWD
-- 按 order_id 取最新事件做 upsert。只扫最近 ${days} 天的分区，靠 Iceberg 的分区裁剪控制扫描量。
--
-- 列必须显式列出，不能用 UPDATE SET * / INSERT *：silver 会被 EvolveJob 加列（如 channel），
-- 而 source 只有 ODS 的 11 列，用 * 时 target 多出来的列解析不到，transform 直接失败。
-- 显式列下，target 新增列在 MATCHED 时保留原值、在 NOT MATCHED 时填 NULL。
--
-- @lineage table: bronze_orders -> silver_orders
-- @lineage column: bronze_orders.order_id -> silver_orders.order_id
-- @lineage column: bronze_orders.user_id -> silver_orders.user_id
-- @lineage column: bronze_orders.merchant_id -> silver_orders.merchant_id
-- @lineage column: bronze_orders.category -> silver_orders.category
-- @lineage column: bronze_orders.amount -> silver_orders.amount
-- @lineage column: bronze_orders.currency -> silver_orders.currency
-- @lineage column: bronze_orders.status -> silver_orders.status
-- @lineage column: bronze_orders.status -> silver_orders.is_refund
-- @lineage column: bronze_orders.event_time -> silver_orders.dt
-- @lineage column: bronze_orders.event_time -> silver_orders.event_time
MERGE INTO ${silver_table} AS t
USING (
  SELECT order_id, user_id, merchant_id, category, amount, currency, status,
         status = 'REFUNDED' AS is_refund,
         CAST(event_time AS DATE) AS dt,
         event_time, CURRENT_TIMESTAMP() AS updated_at
  FROM (
    SELECT b.*, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_time DESC, ingest_time DESC) AS rn
    FROM ${bronze_table} b
    WHERE event_time >= CURRENT_DATE() - INTERVAL ${days} DAYS
  ) WHERE rn = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.event_time > t.event_time THEN UPDATE SET
  user_id = s.user_id,
  merchant_id = s.merchant_id,
  category = s.category,
  amount = s.amount,
  currency = s.currency,
  status = s.status,
  is_refund = s.is_refund,
  dt = s.dt,
  event_time = s.event_time,
  updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (
  order_id, user_id, merchant_id, category, amount, currency, status,
  is_refund, dt, event_time, updated_at
) VALUES (
  s.order_id, s.user_id, s.merchant_id, s.category, s.amount, s.currency, s.status,
  s.is_refund, s.dt, s.event_time, s.updated_at
);
