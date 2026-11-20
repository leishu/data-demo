-- 品类退款率（近 7 天）—— 明细层直接扫，验证 Trino 读 DWD 明细的即席能力
-- 这条经 TrinoDialect 把 date_add('day', -7, ...) 改写成 date_sub(...) 后，Spark 也能校验
SELECT category,
       COUNT(*)                                              AS orders,
       SUM(CASE WHEN is_refund THEN 1 ELSE 0 END)            AS refunds,
       ROUND(SUM(CASE WHEN is_refund THEN 1.0 ELSE 0 END)
             * 100 / COUNT(*), 2)                            AS refund_rate_pct,
       ROUND(SUM(amount), 2)                                 AS amount
FROM iceberg.lake.silver_orders
WHERE event_time >= DATE_ADD('day', -7, CURRENT_DATE)
GROUP BY category
ORDER BY amount DESC;
