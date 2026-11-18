-- 品类退款率（近 7 天）—— 明细层直接扫，验证 Presto 读 DWD 明细的即席能力
-- 这条经 PrestoDialect 把 date_add('day', -7, ...) 改写成 date_sub(...) 后，Spark 也能校验
SELECT category,
       count(*)                                              AS orders,
       sum(CASE WHEN is_refund THEN 1 ELSE 0 END)            AS refunds,
       round(sum(CASE WHEN is_refund THEN 1.0 ELSE 0 END)
             * 100 / count(*), 2)                            AS refund_rate_pct,
       round(sum(amount), 2)                                 AS amount
FROM iceberg.lake.silver_orders
WHERE event_time >= date_add('day', -7, current_date)
GROUP BY category
ORDER BY amount DESC;
