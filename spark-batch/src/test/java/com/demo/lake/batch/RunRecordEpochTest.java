package com.demo.lake.batch;

import com.demo.lake.batch.job.InitJob;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.cli.Args;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 运行记录的时间语义回归：新行直接写 epoch 毫秒；老表由 InitJob 补列并按 UTC 墙钟回填。
 * 这样读取侧不需要任何时区假设，历史列表不会再出现 8 小时偏移。
 */
class RunRecordEpochTest {

    @TempDir
    static Path tempDir;

    private static SparkSession spark;
    private static PlatformConfig cfg;
    private static TableRef runs;

    @BeforeAll
    static void setUp() {
        System.setProperty("platform.warehouse", tempDir.resolve("warehouse").toString());
        System.setProperty("platform.catalog.type", "hadoop");
        System.setProperty("platform.database", "lake_epoch_test");
        System.setProperty("platform.spark.shuffle.partitions", "2");
        cfg = PlatformConfig.load();
        spark = SparkSessionFactory.create(cfg, "run-record-epoch-test");
        runs = TableRef.of(cfg.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.BATCH_RUNS);

        // 故意先建「老结构」的表：没有 epoch 列，模拟升级前的仓库
        spark.sql("CREATE DATABASE IF NOT EXISTS " + cfg.catalogName() + "." + LakeTables.OPS_SCHEMA);
        spark.sql("CREATE TABLE IF NOT EXISTS " + runs.fqn() + " (\n"
                + " run_id STRING, job STRING, command STRING, status STRING, exit_code INT,\n"
                + " started_at TIMESTAMP, finished_at TIMESTAMP, duration_ms BIGINT,\n"
                + " log_file STRING, message STRING\n"
                + ") USING iceberg TBLPROPERTIES ('format-version' = '2')");
        spark.sql("INSERT INTO " + runs.fqn() + " VALUES "
                + "('legacy', 'init', '', 'SUCCEEDED', 0,"
                + " timestamp'2026-01-02 03:04:05', timestamp'2026-01-02 03:04:10', 5000, '', '')");
        // 先完成升级，保证两个测试都面对新结构；回填结果在测试里断言
        new InitJob().run(spark, cfg, Args.parse(new String[]{"--job", "init"}));
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void 老表补列后按UTC墙钟回填epoch() {
        Row row = spark.sql("select started_ms, finished_ms from " + runs.fqn()
                + " where run_id = 'legacy'").head();
        assertEquals(Instant.parse("2026-01-02T03:04:05Z").toEpochMilli(), row.getLong(0));
        assertEquals(Instant.parse("2026-01-02T03:04:10Z").toEpochMilli(), row.getLong(1));
    }

    @Test
    void 新运行记录直接写权威epoch列() {
        Instant start = Instant.parse("2026-02-03T04:05:06.789Z");
        RunRecordWriter.record(spark, cfg, Args.parse(new String[]{"--job", "init"}),
                "epoch-run", "--job init", "SUCCEEDED", 0, start, "ok");

        Row row = spark.sql("select started_ms from " + runs.fqn()
                + " where run_id = 'epoch-run'").head();
        assertEquals(start.toEpochMilli(), row.getLong(0));
    }
}
