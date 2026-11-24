-- Spark SQL / Iceberg —— DWD：与 ODS 同粒度，已清洗去重，追加派生列 is_refund / dt。
CREATE TABLE IF NOT EXISTS ${catalog}.${database}.silver_orders (
  order_id     STRING      COMMENT '订单号(主键)',
  user_id      STRING      COMMENT '用户号',
  merchant_id  STRING      COMMENT '商户号',
  category     STRING      COMMENT '品类',
  amount       DECIMAL(12,2) COMMENT '订单金额',
  currency     STRING      COMMENT '币种',
  status       STRING      COMMENT 'PAID/REFUNDED/CANCELLED',
  is_refund    BOOLEAN     COMMENT '是否退款',
  dt           DATE        COMMENT '业务日期(UTC)',
  event_time   TIMESTAMP   COMMENT '业务发生时间',
  updated_at   TIMESTAMP   COMMENT '最后更新时间'
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
${table_properties}
);
