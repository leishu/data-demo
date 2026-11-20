-- 商户 GMV 排行（近 7 天）
SELECT merchant_id,
       SUM(gmv)         AS gmv,
       SUM(net_revenue) AS net_revenue,
       SUM(order_cnt)   AS orders,
       ROUND(AVG(gmv), 2) AS avg_daily_gmv
FROM iceberg.lake.gold_daily_revenue
WHERE dt >= DATE_ADD('day', -6, CURRENT_DATE)
GROUP BY merchant_id
ORDER BY gmv DESC
LIMIT 10;
