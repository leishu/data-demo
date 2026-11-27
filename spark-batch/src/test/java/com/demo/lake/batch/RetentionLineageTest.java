package com.demo.lake.batch;

import com.demo.lake.batch.job.RetentionJob;
import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.cli.Args;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.governance.GovernanceCatalog;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 真实 Spark：写入血缘落表 + 保留作业 dry-run 不删、execute 删旧分区并留审计 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RetentionLineageTest {

    @TempDir
    static Path tempDir;

    private static SparkSession spark;
    private static PlatformConfig platformConfig;
    private static TableRef bronze;
    private static TableRef silver;
    private static TableRef gold;
    private static TableRef lineage;
    private static TableRef audit;

    @BeforeAll
    static void setUp() {
        System.setProperty("platform.warehouse", tempDir.resolve("warehouse").toString());
        System.setProperty("platform.catalog.type", "hadoop");
        System.setProperty("platform.database", "lake_retention_test");
        System.setProperty("platform.spark.shuffle.partitions", "2");
        platformConfig = PlatformConfig.load();
        spark = SparkSessionFactory.create(platformConfig, "retention-lineage-test");
        bronze = TableRef.of(platformConfig, LakeTables.BRONZE_ORDERS);
        silver = TableRef.of(platformConfig, LakeTables.SILVER_ORDERS);
        gold = TableRef.of(platformConfig, LakeTables.GOLD_DAILY_REVENUE);
        lineage = TableRef.of(platformConfig.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.LINEAGE_RUNS);
        audit = TableRef.of(platformConfig.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.RETENTION_AUDIT);

        LakeTables.all(platformConfig).forEach(ddl -> spark.sql(ddl));
        String old = "CAST(date_sub(current_date(), 10) AS TIMESTAMP)";
        String recent = "CAST(date_sub(current_date(), 1) AS TIMESTAMP)";
        spark.sql("INSERT INTO " + bronze.fqn() + " VALUES "
                + "('R-OLD','U1','M1','3C',CAST(10 AS DECIMAL(12,2)),'CNY','PAID'," + old + ",current_timestamp(),'f','run-x'),"
                + "('R-NEW','U2','M1','3C',CAST(20 AS DECIMAL(12,2)),'CNY','PAID'," + recent + ",current_timestamp(),'f','run-x')");
        spark.sql("INSERT INTO " + silver.fqn() + " (order_id, user_id, merchant_id, category, amount, currency, "
                + "status, is_refund, dt, event_time, updated_at) VALUES "
                + "('R-OLD','U1','M1','3C',CAST(10 AS DECIMAL(12,2)),'CNY','PAID',false,"
                + "date_sub(current_date(),10)," + old + ",current_timestamp()),"
                + "('R-NEW','U2','M1','3C',CAST(20 AS DECIMAL(12,2)),'CNY','PAID',false,"
                + "date_sub(current_date(),1)," + recent + ",current_timestamp())");
        spark.sql("INSERT INTO " + gold.fqn() + " (dt, merchant_id, category, order_cnt, buyer_cnt, gmv, "
                + "refund_amount, net_revenue, updated_at) VALUES "
                + "(date_sub(current_date(),10),'M1','3C',1,1,CAST(10 AS DECIMAL(18,2)),"
                + "CAST(0 AS DECIMAL(18,2)),CAST(10 AS DECIMAL(18,2)),current_timestamp()),"
                + "(date_sub(current_date(),1),'M1','3C',1,1,CAST(20 AS DECIMAL(18,2)),"
                + "CAST(0 AS DECIMAL(18,2)),CAST(20 AS DECIMAL(18,2)),current_timestamp())");
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    @Order(1)
    void 写入血缘按作业记录表与快照() {
        // snapshot_id 是 UUID 随机数，不能靠 max 找最新；排序键必须是 committed_at
        assertTrue(SparkSnapshots.LATEST_SNAPSHOT.contains("ORDER BY committed_at DESC"));
        assertFalse(SparkSnapshots.LATEST_SNAPSHOT.contains("max("));

        LineageWriter.record(spark, platformConfig, "run-lineage", LakeJobs.TRANSFORM);
        List<Row> rows = spark.sql("SELECT table_name, snapshot_id FROM " + lineage.fqn()
                + " WHERE run_id = 'run-lineage'").collectAsList();

        assertEquals(2, rows.size());
        List<String> tables = new java.util.ArrayList<>();
        for (Row row : rows) {
            String table = row.getString(0);
            tables.add(table);
            String recorded = row.getString(1);
            assertTrue(recorded != null && !recorded.isEmpty(), "快照 ID 不应为空");
            TableRef ref = LakeTables.SILVER_ORDERS.equals(table) ? silver : gold;
            assertEquals(SparkSnapshots.latestId(spark, ref.fqn()), recorded,
                    table + " 应记录最近一次提交的快照，而不是随机 max");
        }
        assertTrue(tables.containsAll(Arrays.asList(LakeTables.SILVER_ORDERS, LakeTables.GOLD_DAILY_REVENUE)));
        assertEquals(3, GovernanceCatalog.ASSETS.size());
    }

    @Test
    @Order(2)
    void dryRun列出待删分区但不删数据() {
        Args args = Args.parse(new String[]{"--job", LakeJobs.RETENTION, "--run-id", "run-ret-dry",
                "--retention-days", "3"});
        new RetentionJob().run(spark, platformConfig, args);

        assertEquals(2L, spark.table(bronze.fqn()).count(), "dry-run 不应删数据");
        List<Row> events = spark.sql("SELECT event, dry_run, partitions, row_count FROM " + audit.fqn()
                + " WHERE run_id = 'run-ret-dry'").collectAsList();
        assertEquals(6, events.size(), "三张表各写前后两条审计事件");
        for (Row row : events) {
            assertTrue(row.getBoolean(1), "dry-run 事件应标记 dry_run=true");
        }
        assertTrue(events.stream().anyMatch(r -> "retention.done".equals(r.getString(0))
                && r.getInt(2) > 0 && r.getLong(3) > 0), "应列出待删分区与行数");
    }

    @Test
    @Order(3)
    void execute删旧分区且表结构不变() {
        List<String> before = Arrays.asList(spark.table(bronze.fqn()).columns());
        Args args = Args.parse(new String[]{"--job", LakeJobs.RETENTION, "--run-id", "run-ret-exec",
                "--retention-days", "3", "--execute"});
        new RetentionJob().run(spark, platformConfig, args);

        assertEquals(1L, spark.table(bronze.fqn()).count(), "10 天前的分区应被删掉");
        assertEquals(1L, spark.table(silver.fqn()).count());
        assertEquals(1L, spark.table(gold.fqn()).count());
        assertEquals(before, Arrays.asList(spark.table(bronze.fqn()).columns()), "不应改动业务表结构");

        List<Row> events = spark.sql("SELECT event, dry_run, partitions, row_count FROM " + audit.fqn()
                + " WHERE run_id = 'run-ret-exec'").collectAsList();
        assertEquals(6, events.size());
        for (Row row : events) {
            assertTrue(!row.getBoolean(1), "execute 事件应标记 dry_run=false");
        }
        assertTrue(events.stream().anyMatch(r -> "retention.done".equals(r.getString(0)) && r.getInt(2) > 0),
                "执行后应记录命中的分区数");
    }
}
