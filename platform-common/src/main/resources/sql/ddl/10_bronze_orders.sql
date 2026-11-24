-- Spark SQL / Iceberg —— ODS：原始订单落地，只追加不做更新。
-- run_id 记录本条数据由哪次 ingest 写入，可直接 join ops.batch_runs 回溯批次。
-- 早期版本的表没有 run_id，由 IngestJob 首次运行时 ADD COLUMNS 补上。
CREATE TABLE IF NOT EXISTS ${catalog}.${database}.bronze_orders (
  order_id     STRING      COMMENT '订单号',
  user_id      STRING      COMMENT '用户号',
  merchant_id  STRING      COMMENT '商户号',
  category     STRING      COMMENT '品类',
  amount       DECIMAL(12,2) COMMENT '订单金额',
  currency     STRING      COMMENT '币种',
  status       STRING      COMMENT 'PAID/REFUNDED/CANCELLED',
  event_time   TIMESTAMP   COMMENT '业务发生时间(分区字段来源)',
  ingest_time  TIMESTAMP   COMMENT '入湖时间',
  source_file  STRING      COMMENT '来源文件',
  run_id       STRING      COMMENT '入湖批次运行ID（对应 ops.batch_runs.run_id）'
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
${table_properties}
);
