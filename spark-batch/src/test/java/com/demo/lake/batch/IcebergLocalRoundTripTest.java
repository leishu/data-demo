package com.demo.lake.batch;

import com.demo.lake.batch.job.IngestJob;
import com.demo.lake.common.batch.BatchRunStatus;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.catalog.TransformSql;
import com.demo.lake.common.cli.Args;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用本地 Hadoop catalog 跑一遍「建表 -> 入湖 -> MERGE 去重 -> 汇总 -> 加列 -> 维护」，
 * 验证 Spark + Iceberg 这套依赖在 JDK11 下确实能跑通（不依赖任何外部服务）。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IcebergLocalRoundTripTest {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @TempDir
    static Path tempDir;

    private static SparkSession spark;
    private static PlatformConfig cfg;
    private static TableRef bronze;
    private static TableRef silver;
    private static TableRef gold;

    @BeforeAll
    static void setUp() {
        System.setProperty("platform.warehouse", tempDir.resolve("warehouse").toString());
        System.setProperty("platform.catalog.type", "hadoop");
        System.setProperty("platform.database", "lake_test");
        System.setProperty("platform.spark.shuffle.partitions", "2");
        System.setProperty("platform.raw.dir", tempDir.resolve("raw").toString());
        cfg = PlatformConfig.load();

        spark = SparkSessionFactory.create(cfg, "iceberg-roundtrip-test");
        bronze = TableRef.of(cfg, LakeTables.BRONZE_ORDERS);
        silver = TableRef.of(cfg, LakeTables.SILVER_ORDERS);
        gold = TableRef.of(cfg, LakeTables.GOLD_DAILY_REVENUE);
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    @Order(1)
    void createsTablesAndIngestsDuplicateRows() {
        LakeTables.all(cfg).forEach(ddl -> spark.sql(ddl));

        // O1 出现两次（先 PAID 后 REFUNDED），O3 是取消单
        // 时间固定到 10:00/12:00，避免测试在 UTC 深夜跨天导致分区漂移
        spark.sql("INSERT INTO " + bronze.fqn() + " VALUES "
                + "('O1','U1','M1','3C',   CAST(100.00 AS DECIMAL(12,2)), 'CNY', 'PAID',"
                + " to_timestamp(concat(cast(date_sub(current_date(), 1) AS string), ' 10:00:00')),"
                + " current_timestamp(), 'f1', 'run-unit-1'),"
                + "('O1','U1','M1','3C',   CAST(100.00 AS DECIMAL(12,2)), 'CNY', 'REFUNDED',"
                + " to_timestamp(concat(cast(date_sub(current_date(), 1) AS string), ' 12:00:00')),"
                + " current_timestamp(), 'f1', 'run-unit-1'),"
                + "('O2','U2','M1','3C',   CAST(50.00  AS DECIMAL(12,2)), 'CNY', 'PAID',"
                + " to_timestamp(concat(cast(date_sub(current_date(), 1) AS string), ' 11:00:00')),"
                + " current_timestamp(), 'f1', 'run-unit-1'),"
                + "('O3','U3','M2','HOME', CAST(30.00  AS DECIMAL(12,2)), 'CNY', 'CANCELLED',"
                + " to_timestamp(concat(cast(date_sub(current_date(), 2) AS string), ' 09:00:00')),"
                + " current_timestamp(), 'f1', 'run-unit-1')");

        assertEquals(4L, spark.table(bronze.fqn()).count());
        assertEquals(4L, spark.sql("SELECT count(*) FROM " + bronze.fqn()
                + " WHERE run_id = 'run-unit-1'").head().getLong(0), "每行都应带写入批次");
        assertEquals(1L, spark.sql("SELECT count(*) FROM " + bronze.metadata("snapshots")).head().getLong(0));
        // 分区由 Iceberg 按 days(event_time) 路由，应至少有 2 个分区
        assertTrue(spark.sql("SELECT count(*) FROM " + bronze.metadata("partitions")).head().getLong(0) >= 2);
    }

    @Test
    @Order(2)
    void mergeDedupesAndAggregates() {
        spark.sql(TransformSql.mergeBronzeIntoSilver(cfg, 7));

        assertEquals(3L, spark.table(silver.fqn()).count(), "O1 的两条应合并为一条");
        Row o1 = spark.sql("SELECT status, is_refund FROM " + silver.fqn() + " WHERE order_id = 'O1'").head();
        assertEquals("REFUNDED", o1.getString(0), "应取 event_time 最新的那条");
        assertTrue(o1.getBoolean(1));

        spark.sql(TransformSql.rebuildGold(cfg));
        // 汇总一次，避免按单行断言依赖分区边界
        Row m1 = spark.sql("SELECT sum(order_cnt), sum(gmv), sum(refund_amount), sum(net_revenue) "
                + "FROM " + gold.fqn() + " WHERE merchant_id = 'M1' AND category = '3C'").head();
        assertEquals(2L, m1.getLong(0), "取消单不参与 GMV");
        assertEquals(0, new BigDecimal("150.00").compareTo(m1.getDecimal(1)));
        assertEquals(0, new BigDecimal("100.00").compareTo(m1.getDecimal(2)));
        assertEquals(0, new BigDecimal("50.00").compareTo(m1.getDecimal(3)));
    }

    @Test
    @Order(3)
    void schemaEvolutionAddsColumnWithoutRewritingHistory() {
        spark.sql("ALTER TABLE " + silver.fqn() + " ADD COLUMNS (channel STRING COMMENT '下单渠道')");
        assertTrue(List.of(spark.table(silver.fqn()).columns()).contains("channel"));

        spark.sql("UPDATE " + silver.fqn() + " SET channel = "
                + "CASE pmod(hash(user_id), 3) WHEN 0 THEN 'APP' WHEN 1 THEN 'MINI_APP' ELSE 'WEB' END");

        assertEquals(0L, spark.table(silver.fqn()).filter("channel IS NULL").count());
        assertTrue(spark.sql("SELECT count(*) FROM " + silver.metadata("snapshots")).head().getLong(0) >= 2,
                "加列 + 回填应各自产生快照");
    }

    @Test
    @Order(4)
    void maintenanceProceduresWork() {
        spark.sql("CALL " + cfg.catalogName() + ".system.rewrite_data_files("
                + "table => '" + cfg.database() + "." + LakeTables.SILVER_ORDERS + "', "
                + "options => map('min-input-files', '2', 'target-file-size-bytes', '1048576'))");

        spark.sql("CALL " + cfg.catalogName() + ".system.rewrite_manifests("
                + "table => '" + cfg.database() + "." + LakeTables.SILVER_ORDERS + "')");

        String olderThan = LocalDateTime.now(ZoneOffset.UTC).plusMinutes(1).format(TS);
        spark.sql("CALL " + cfg.catalogName() + ".system.expire_snapshots("
                + "table => '" + cfg.database() + "." + LakeTables.SILVER_ORDERS + "', "
                + "older_than => TIMESTAMP '" + olderThan + "', retain_last => 1)");

        assertEquals(1L, spark.sql("SELECT count(*) FROM " + silver.metadata("snapshots")).head().getLong(0));
        assertEquals(3L, spark.table(silver.fqn()).count(), "清理快照不应影响数据");
    }

    @Test
    @Order(5)
    void runRecordLandsInOpsTable() {
        Args args = Args.parse(new String[]{"--job", "client-test"});
        RunRecordWriter.record(spark, cfg, args, "run-unit-1", "--job client-test",
                BatchRunStatus.SUCCEEDED, 0, Instant.now().minusSeconds(1), "完成");

        TableRef ops = TableRef.of(cfg.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.BATCH_RUNS);
        Row row = spark.sql("SELECT run_id, job, status, exit_code, log_file FROM " + ops.fqn()
                + " WHERE run_id = 'run-unit-1'").head();
        assertEquals("run-unit-1", row.getString(0));
        assertEquals("client-test", row.getString(1));
        assertEquals(BatchRunStatus.SUCCEEDED.name(), row.getString(2));
        assertEquals(0, row.getInt(3));
        assertTrue(row.isNullAt(4), "命令行跑的作业没有日志文件");
    }

    @Test
    @Order(6)
    void ingestBackfillsRunIdAndDropsLegacyRows() throws Exception {
        // 模拟升级前的老表：没有 run_id 列，且已有一行历史数据
        spark.sql("DROP TABLE IF EXISTS " + bronze.fqn());
        spark.sql("CREATE TABLE " + bronze.fqn() + " ("
                + "order_id STRING, user_id STRING, merchant_id STRING, category STRING,"
                + "amount DECIMAL(12,2), currency STRING, status STRING, event_time TIMESTAMP,"
                + "ingest_time TIMESTAMP, source_file STRING)"
                + " USING iceberg PARTITIONED BY (days(event_time))");
        spark.sql("INSERT INTO " + bronze.fqn() + " VALUES ('LEGACY','U0','M0','3C',"
                + " CAST(1.00 AS DECIMAL(12,2)), 'CNY', 'PAID', current_timestamp(), current_timestamp(), 'old')");

        Args args = Args.parse(new String[]{"--days", "1", "--orders-per-day", "20",
                "--run-id", "run-legacy-upgrade"});
        new IngestJob().run(spark, cfg, args);

        assertTrue(List.of(spark.table(bronze.fqn()).columns()).contains("run_id"), "旧表应被补上 run_id 列");
        assertEquals(0L, spark.sql("SELECT count(*) FROM " + bronze.fqn()
                + " WHERE order_id = 'LEGACY'").head().getLong(0), "补列前的历史行应被清掉");
        assertTrue(spark.sql("SELECT count(*) FROM " + bronze.fqn()
                + " WHERE run_id = 'run-legacy-upgrade'").head().getLong(0) > 0, "本次写入的行应带同一个 run_id");
        assertEquals(0L, spark.table(bronze.fqn()).filter("run_id IS NULL").count(), "表里不应残留无法归属的行");
    }
}
