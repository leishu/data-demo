-- 渠道结构（近 7 天）
-- 前置条件：先执行过 schema 演进，即 mvn ... --job evolve（否则 channel 列不存在）
SELECT channel,
       COUNT(*)              AS orders,
       ROUND(SUM(amount), 2) AS amount,
       COUNT(DISTINCT user_id) AS buyers
FROM iceberg.lake.silver_orders
WHERE event_time >= DATE_ADD('day', -7, CURRENT_DATE)
GROUP BY channel
ORDER BY amount DESC;
