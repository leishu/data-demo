-- Spark SQL / Iceberg —— ADS：按天 + 商户 + 品类聚合，天作为分区，支持按分区覆盖重算。
CREATE TABLE IF NOT EXISTS ${catalog}.${database}.gold_daily_revenue (
  dt             DATE        COMMENT '业务日期',
  merchant_id    STRING      COMMENT '商户号',
  category       STRING      COMMENT '品类',
  order_cnt      BIGINT      COMMENT '有效订单数',
  buyer_cnt      BIGINT      COMMENT '本商户+品类下的下单用户数(跨行不可相加)',
  gmv            DECIMAL(18,2) COMMENT '成交总额',
  refund_amount  DECIMAL(18,2) COMMENT '退款金额',
  net_revenue    DECIMAL(18,2) COMMENT '净收入=GMV-退款',
  updated_at     TIMESTAMP   COMMENT '最后更新时间'
)
USING iceberg
PARTITIONED BY (dt)
TBLPROPERTIES (
${table_properties}
);
