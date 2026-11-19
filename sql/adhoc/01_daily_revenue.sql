-- 每日大盘：GMV / 退款 / 净收入（近 7 天，按天）
-- 数据源：ADS 层汇总表，Trino 直接读 Iceberg，不经过 Spark
-- 注意：buyer_cnt 是 (商户,品类) 粒度去重，sum 得到的是“人次”，不是全站去重买家数
SELECT dt,
       sum(order_cnt)                              AS orders,
       sum(buyer_cnt)                              AS buyer_cnt_sum,
       sum(gmv)                                    AS gmv,
       sum(refund_amount)                          AS refund_amount,
       sum(net_revenue)                            AS net_revenue,
       round(sum(refund_amount) / sum(gmv) * 100, 2) AS refund_rate_pct
FROM iceberg.lake.gold_daily_revenue
WHERE dt >= date_add('day', -6, current_date)
GROUP BY dt
ORDER BY dt;
