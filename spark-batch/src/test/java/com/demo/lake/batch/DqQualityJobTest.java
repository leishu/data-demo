package com.demo.lake.batch;

import com.demo.lake.batch.job.QualityJob;
import com.demo.lake.common.batch.LakeJobs;
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

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 真实建表 + 真实写入：干净数据下质量检查通过；注入重复单/负金额后检查写出失败证据并让作业失败。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DqQualityJobTest {

    @TempDir
    static Path tempDir;

    private static SparkSession spark;
    private static PlatformConfig cfg;
    private static TableRef results;

    @BeforeAll
    static void setUp() {
        System.setProperty("platform.warehouse", tempDir.resolve("warehouse").toString());
        System.setProperty("platform.catalog.type", "hadoop");
        System.setProperty("platform.database", "lake_dq_test");
        System.setProperty("platform.spark.shuffle.partitions", "2");
        cfg = PlatformConfig.load();
        spark = SparkSessionFactory.create(cfg, "dq-quality-test");
        results = TableRef.of(cfg.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.DQ_RESULTS);

        LakeTables.all(cfg).forEach(ddl -> spark.sql(ddl));
        TableRef bronze = TableRef.of(cfg, LakeTables.BRONZE_ORDERS);
        String day = "date_sub(current_date(), 1)";
        spark.sql("INSERT INTO " + bronze.fqn() + " VALUES "
                + "('Q1','U1','M1','3C', CAST(100.00 AS DECIMAL(12,2)), 'CNY','PAID',"
                + " to_timestamp(concat(cast(" + day + " AS string), ' 10:00:00')),"
                + " current_timestamp(), 'f1', 'run-dq-fixture'),"
                + "('Q2','U2','M1','3C', CAST(100.00 AS DECIMAL(12,2)), 'CNY','REFUNDED',"
                + " to_timestamp(concat(cast(" + day + " AS string), ' 11:00:00')),"
                + " current_timestamp(), 'f1', 'run-dq-fixture')");
        spark.sql(TransformSql.mergeBronzeIntoSilver(cfg, 7));
        spark.sql(TransformSql.rebuildGold(cfg));
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    @Order(1)
    void 干净数据下六项检查全部通过并落库() {
        Args args = Args.parse(new String[]{"--job", LakeJobs.QUALITY, "--run-id", "dq-clean-run"});
        new QualityJob().run(spark, cfg, args);

        List<Row> rows = spark.sql("SELECT `check`, severity, observed, threshold FROM " + results.fqn()
                + " WHERE run_id = 'dq-clean-run'").collectAsList();
        assertEquals(6, rows.size());
        for (Row row : rows) {
            assertTrue(row.getDouble(2) <= row.getDouble(3),
                    row.getString(0) + " 应通过: observed=" + row.getDouble(2));
        }

        // 证据要指向被检查来源表最近一次提交，不能是随机 max(snapshot_id)
        List<Row> snapshots = spark.sql("SELECT `check`, snapshot_id FROM " + results.fqn()
                + " WHERE run_id = 'dq-clean-run'").collectAsList();
        for (Row row : snapshots) {
            TableRef source = row.getString(0).startsWith("silver_")
                    ? TableRef.of(cfg, LakeTables.SILVER_ORDERS)
                    : TableRef.of(cfg, LakeTables.GOLD_DAILY_REVENUE);
            assertEquals(SparkSnapshots.latestId(spark, source.fqn()), row.getString(1),
                    row.getString(0) + " 的快照应指向最近一次提交");
        }
    }

    @Test
    @Order(2)
    void 注入重复单和负金额后作业失败且结果表留下证据() {
        Args args = Args.parse(new String[]{"--job", LakeJobs.QUALITY, "--run-id", "dq-fault-run",
                "--inject-faults"});
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> new QualityJob().run(spark, cfg, args));
        assertTrue(failure.getMessage().contains("数据质量检查失败"), failure.getMessage());

        List<Row> failed = spark.sql("SELECT `check`, severity, observed, threshold, detail FROM "
                + results.fqn() + " WHERE run_id = 'dq-fault-run' AND observed > threshold").collectAsList();
        assertEquals(2, failed.size(), "重复单 + 负金额应各命中一项 ERROR");
        for (Row row : failed) {
            assertEquals("ERROR", row.getString(1));
        }
        assertTrue(failed.stream().anyMatch(r -> "silver_unique".equals(r.getString(0))
                && r.getString(4).contains("DQ-DUP")), "唯一性失败应带上重复订单号证据");
        assertTrue(failed.stream().anyMatch(r -> "silver_amount_range".equals(r.getString(0))
                && r.getString(4).contains("DQ-NEG")), "金额范围失败应带上负金额订单号证据");
    }
}
