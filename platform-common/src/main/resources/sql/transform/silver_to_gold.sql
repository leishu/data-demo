-- Spark SQL / Iceberg —— DWD -> ADS
-- INSERT OVERWRITE 在 Iceberg 里是动态分区覆盖，只替换出现的分区；按天增量时在 WHERE 加 dt 条件。
-- buyer_cnt 是 (dt, 商户, 品类) 粒度下的去重用户数，跨行相加得到的是“人次”，不是全站去重买家数。
-- 列必须显式列出：不写列列表时 INSERT OVERWRITE 靠位置匹配，gold 一旦加列（或列顺序调整）就会失败。
-- 未列出的新增列保持 NULL，由后续迁移/回填决定语义。
--
-- @lineage table: silver_orders -> gold_daily_revenue
-- @lineage column: silver_orders.dt -> gold_daily_revenue.dt
-- @lineage column: silver_orders.merchant_id -> gold_daily_revenue.merchant_id
-- @lineage column: silver_orders.category -> gold_daily_revenue.category
-- @lineage column: silver_orders.order_id -> gold_daily_revenue.order_cnt
-- @lineage column: silver_orders.status -> gold_daily_revenue.order_cnt
-- @lineage column: silver_orders.user_id -> gold_daily_revenue.buyer_cnt
-- @lineage column: silver_orders.amount -> gold_daily_revenue.gmv
-- @lineage column: silver_orders.amount -> gold_daily_revenue.refund_amount
-- @lineage column: silver_orders.is_refund -> gold_daily_revenue.refund_amount
-- @lineage column: silver_orders.amount -> gold_daily_revenue.net_revenue
-- @lineage column: silver_orders.is_refund -> gold_daily_revenue.net_revenue
INSERT OVERWRITE ${gold_table} (
  dt, merchant_id, category, order_cnt, buyer_cnt, gmv, refund_amount, net_revenue, updated_at
)
SELECT dt, merchant_id, category,
       COUNT(*) AS order_cnt,
       COUNT(DISTINCT user_id) AS buyer_cnt,
       CAST(SUM(amount) AS DECIMAL(18,2)) AS gmv,
       CAST(SUM(CAST(CASE WHEN is_refund THEN amount ELSE CAST(0 AS DECIMAL(12,2)) END
  AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS refund_amount,
       CAST(SUM(CASE WHEN is_refund THEN CAST(0 AS DECIMAL(12,2)) ELSE amount END)
  AS DECIMAL(18,2)) AS net_revenue,
       CURRENT_TIMESTAMP() AS updated_at
FROM ${silver_table}
WHERE status <> 'CANCELLED'
GROUP BY dt, merchant_id, category;
