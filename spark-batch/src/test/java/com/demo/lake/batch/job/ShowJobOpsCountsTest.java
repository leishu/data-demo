package com.demo.lake.batch.job;

import com.demo.lake.batch.SparkSessionFactory;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 真实建表：show 巡检的四张 ops 表计数要覆盖全部元数据表，且行数跟着数据变。
 */
class ShowJobOpsCountsTest {

    @TempDir
    static Path tempDir;

    private static SparkSession spark;
    private static PlatformConfig platformConfig;

    @BeforeAll
    static void setUp() {
        System.setProperty("platform.warehouse", tempDir.resolve("warehouse").toString());
        System.setProperty("platform.catalog.type", "hadoop");
        System.setProperty("platform.database", "lake_show_ops_test");
        System.setProperty("platform.spark.shuffle.partitions", "2");
        platformConfig = PlatformConfig.load();
        spark = SparkSessionFactory.create(platformConfig, "show-ops-counts-test");
        LakeTables.all(platformConfig).forEach(ddl -> spark.sql(ddl));
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void 四张ops表都进巡检且行数反映真实数据() {
        List<String> empty = ShowJob.opsCountLines(spark, platformConfig);
        assertEquals(4, empty.size());
        assertTrue(empty.get(0).contains(LakeTables.BATCH_RUNS));
        assertTrue(empty.get(1).contains(LakeTables.DQ_RESULTS));
        assertTrue(empty.get(2).contains(LakeTables.LINEAGE_RUNS));
        assertTrue(empty.get(3).contains(LakeTables.RETENTION_AUDIT));
        for (String line : empty) {
            assertTrue(line.trim().endsWith("0 行"), line);
        }

        TableRef runs = TableRef.of(platformConfig.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.BATCH_RUNS);
        spark.sql("INSERT INTO " + runs.fqn() + " (run_id, job, command, status, exit_code, "
                + "started_at, finished_at, duration_ms, log_file, message, started_ms, finished_ms) "
                + "VALUES ('show-fixture', 'show', '--job show', 'SUCCEEDED', 0, "
                + "current_timestamp(), current_timestamp(), 1, 'x.log', '完成', 1, 2)");

        List<String> after = ShowJob.opsCountLines(spark, platformConfig);
        assertTrue(after.get(0).trim().endsWith("1 行"), after.get(0));
        assertTrue(after.get(1).trim().endsWith("0 行"), after.get(1));
    }
}
