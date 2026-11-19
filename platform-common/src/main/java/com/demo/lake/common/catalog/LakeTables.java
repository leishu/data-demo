package com.demo.lake.common.catalog;

import com.demo.lake.common.config.PlatformConfig;

import java.util.Arrays;
import java.util.List;

/**
 * 湖仓分层表的 DDL 定义（Spark SQL / Iceberg 语法）。
 *
 * <p>分层：</p>
 * <ul>
 *   <li>ODS  bronze_orders        原始落地，只追加，按 event_time 天分区</li>
 *   <li>DWD  silver_orders        清洗 + 去重 + 派生字段，MERGE 增量更新</li>
 *   <li>ADS  gold_daily_revenue   按天/商户/品类的营收汇总，分区覆盖写</li>
 * </ul>
 *
 * <p>关键表属性说明：merge/update/delete 全部走 copy-on-write。
 * 读取侧（Trino）要处理 MoR 的 delete file，走 CoW 可以让 Spark 写、Trino 读
 * 这条链路零兼容问题（代价是写放大）。</p>
 *
 * <p>另有一个 ops 层（独立 namespace {@value #OPS_SCHEMA}）：{@code ops.batch_runs} 记录批作业
 * 的终态运行历史。放在业务库之外，就不会出现在 lake-service 的表浏览清单里。</p>
 */
public final class LakeTables {

    public static final String BRONZE_ORDERS = "bronze_orders";
    public static final String SILVER_ORDERS = "silver_orders";
    public static final String GOLD_DAILY_REVENUE = "gold_daily_revenue";

    /** 运维元数据 namespace：不进三层业务表清单 */
    public static final String OPS_SCHEMA = "ops";
    /** 批作业运行历史：一行一次运行的终态，由 BatchApp 写、lake-service 读 */
    public static final String BATCH_RUNS = "batch_runs";

    private LakeTables() {
    }

    public static List<String> all(PlatformConfig cfg) {
        return Arrays.asList(
                createDatabase(cfg),
                createOpsDatabase(cfg),
                createBronzeOrders(cfg),
                createSilverOrders(cfg),
                createGoldDailyRevenue(cfg),
                createBatchRuns(cfg));
    }

    /**
     * 注意：这里不能带 COMMENT。
     * Spark 会把 COMMENT 当成 namespace metadata 传给 catalog，而 HadoopCatalog
     * 明确拒绝非空 metadata（"Cannot create namespace: metadata is not supported"）。
     * 去掉后 hadoop / hive 两种 catalog 都能执行。
     */
    public static String createDatabase(PlatformConfig cfg) {
        return "CREATE DATABASE IF NOT EXISTS " + cfg.catalogName() + "." + cfg.database();
    }

    public static String createOpsDatabase(PlatformConfig cfg) {
        return "CREATE DATABASE IF NOT EXISTS " + cfg.catalogName() + "." + OPS_SCHEMA;
    }

    /**
     * ops：批作业运行历史。只落终态（SUCCEEDED / FAILED），不写 RUNNING——
     * 进程被 kill 或超时强杀时没人来收尾，留着 RUNNING 行会变成永久脏数据。
     *
     * <p>不分区：单表行数就是「跑过多少次作业」，量级极小，分区只会让 Trino 侧多一层元数据。</p>
     */
    public static String createBatchRuns(PlatformConfig cfg) {
        TableRef t = TableRef.of(cfg.catalogName(), OPS_SCHEMA, BATCH_RUNS);
        return "CREATE TABLE IF NOT EXISTS " + t.fqn() + " (\n"
                + "  run_id       STRING      COMMENT '运行 ID（服务端生成，与内存记录对齐）',\n"
                + "  job          STRING      COMMENT '作业名：init/ingest/.../all',\n"
                + "  command      STRING      COMMENT '本次进程收到的参数',\n"
                + "  status       STRING      COMMENT 'SUCCEEDED/FAILED',\n"
                + "  exit_code    INT         COMMENT '作业语义退出码：0 成功 1 失败',\n"
                + "  started_at   TIMESTAMP   COMMENT '开始时间(UTC)',\n"
                + "  finished_at  TIMESTAMP   COMMENT '结束时间(UTC)',\n"
                + "  duration_ms  BIGINT      COMMENT '耗时(毫秒)',\n"
                + "  log_file     STRING      COMMENT '服务端日志文件，重启后仍可 tail',\n"
                + "  message      STRING      COMMENT '结果说明'\n"
                + ")\n"
                + "USING iceberg\n"
                + "TBLPROPERTIES (\n"
                + tableProperties(cfg, false)
                + ")";
    }

    /**
     * ODS：原始订单落地表，只追加不做更新。
     *
     * <p>run_id 记录本条数据由哪次 ingest 写入，可直接 join {@code ops.batch_runs} 回溯批次；
     * 早期版本的表没有该列，由 IngestJob 首次运行时 ADD COLUMNS 补上并清掉无法归属的旧行。</p>
     */
    public static String createBronzeOrders(PlatformConfig cfg) {
        TableRef t = TableRef.of(cfg, BRONZE_ORDERS);
        return "CREATE TABLE IF NOT EXISTS " + t.fqn() + " (\n"
                + "  order_id     STRING      COMMENT '订单号',\n"
                + "  user_id      STRING      COMMENT '用户号',\n"
                + "  merchant_id  STRING      COMMENT '商户号',\n"
                + "  category     STRING      COMMENT '品类',\n"
                + "  amount       DECIMAL(12,2) COMMENT '订单金额',\n"
                + "  currency     STRING      COMMENT '币种',\n"
                + "  status       STRING      COMMENT 'PAID/REFUNDED/CANCELLED',\n"
                + "  event_time   TIMESTAMP   COMMENT '业务发生时间(分区字段来源)',\n"
                + "  ingest_time  TIMESTAMP   COMMENT '入湖时间',\n"
                + "  source_file  STRING      COMMENT '来源文件',\n"
                + "  run_id       STRING      COMMENT '入湖批次运行ID（对应 ops.batch_runs.run_id）'\n"
                + ")\n"
                + "USING iceberg\n"
                + "PARTITIONED BY (days(event_time))\n"
                + "TBLPROPERTIES (\n"
                + tableProperties(cfg, true)
                + ")";
    }

    /** DWD：与 ODS 同粒度但已清洗去重，追加派生列 dt / is_refund */
    public static String createSilverOrders(PlatformConfig cfg) {
        TableRef t = TableRef.of(cfg, SILVER_ORDERS);
        return "CREATE TABLE IF NOT EXISTS " + t.fqn() + " (\n"
                + "  order_id     STRING      COMMENT '订单号(主键)',\n"
                + "  user_id      STRING      COMMENT '用户号',\n"
                + "  merchant_id  STRING      COMMENT '商户号',\n"
                + "  category     STRING      COMMENT '品类',\n"
                + "  amount       DECIMAL(12,2) COMMENT '订单金额',\n"
                + "  currency     STRING      COMMENT '币种',\n"
                + "  status       STRING      COMMENT 'PAID/REFUNDED/CANCELLED',\n"
                + "  is_refund    BOOLEAN     COMMENT '是否退款',\n"
                + "  dt           DATE        COMMENT '业务日期(UTC)',\n"
                + "  event_time   TIMESTAMP   COMMENT '业务发生时间',\n"
                + "  updated_at   TIMESTAMP   COMMENT '最后更新时间'\n"
                + ")\n"
                + "USING iceberg\n"
                + "PARTITIONED BY (days(event_time))\n"
                + "TBLPROPERTIES (\n"
                + tableProperties(cfg, true)
                + ")";
    }

    /** ADS：按天 + 商户 + 品类聚合，天作为分区，支持按分区覆盖重算 */
    public static String createGoldDailyRevenue(PlatformConfig cfg) {
        TableRef t = TableRef.of(cfg, GOLD_DAILY_REVENUE);
        return "CREATE TABLE IF NOT EXISTS " + t.fqn() + " (\n"
                + "  dt             DATE        COMMENT '业务日期',\n"
                + "  merchant_id    STRING      COMMENT '商户号',\n"
                + "  category       STRING      COMMENT '品类',\n"
                + "  order_cnt      BIGINT      COMMENT '有效订单数',\n"
                + "  buyer_cnt      BIGINT      COMMENT '本商户+品类下的下单用户数(跨行不可相加)',\n"
                + "  gmv            DECIMAL(18,2) COMMENT '成交总额',\n"
                + "  refund_amount  DECIMAL(18,2) COMMENT '退款金额',\n"
                + "  net_revenue    DECIMAL(18,2) COMMENT '净收入=GMV-退款',\n"
                + "  updated_at     TIMESTAMP   COMMENT '最后更新时间'\n"
                + ")\n"
                + "USING iceberg\n"
                + "PARTITIONED BY (dt)\n"
                + "TBLPROPERTIES (\n"
                + tableProperties(cfg, false)
                + ")";
    }

    private static String tableProperties(PlatformConfig cfg, boolean rowLevelWrites) {
        StringBuilder sb = new StringBuilder();
        sb.append("  'format-version' = '").append(cfg.formatVersion()).append("',\n");
        if (rowLevelWrites) {
            String mode = cfg.writeMode();
            sb.append("  'write.merge.mode' = '").append(mode).append("',\n");
            sb.append("  'write.update.mode' = '").append(mode).append("',\n");
            sb.append("  'write.delete.mode' = '").append(mode).append("',\n");
        }
        sb.append("  'write.target-file-size-bytes' = '134217728',\n");
        sb.append("  'write.metadata.delete-after-commit.enabled' = 'true',\n");
        sb.append("  'write.metadata.previous-versions-max' = '20',\n");
        sb.append("  'comment' = 'lakehouse-demo managed table'");
        return sb.toString();
    }
}
