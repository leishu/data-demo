-- 金额范围：负金额或明显超出上限的订单数必须为 0。上限只是 demo 阈值，按业务调整。
SELECT CAST((
  SELECT count(*) FROM ${silver_table} WHERE amount < 0 OR amount > 1000000
) AS DOUBLE) AS observed,
0.0 AS threshold,
coalesce((
  SELECT concat_ws(', ', collect_set(concat(order_id, ':', CAST(amount AS STRING)))) FROM (
    SELECT order_id, amount FROM ${silver_table}
    WHERE amount < 0 OR amount > 1000000 LIMIT 5
  ) d
), '') AS detail
