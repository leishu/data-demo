-- 商户 GMV 排行（近 7 天）
SELECT merchant_id,
       sum(gmv)         AS gmv,
       sum(net_revenue) AS net_revenue,
       sum(order_cnt)   AS orders,
       round(avg(gmv), 2) AS avg_daily_gmv
FROM iceberg.lake.gold_daily_revenue
WHERE dt >= date_add('day', -6, current_date)
GROUP BY merchant_id
ORDER BY gmv DESC
LIMIT 10;
