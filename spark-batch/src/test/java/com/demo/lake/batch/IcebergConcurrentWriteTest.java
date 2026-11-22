package com.demo.lake.batch;

import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 Iceberg 的乐观并发提交：两个写入者同时 INSERT 同一张表，
 * 要么都成功（Iceberg 内部重试），要么一个成功、另一个拿到 CommitFailedException；
 * 绝不能出现「两边都说成功但只落了一半数据」的静默丢失。
 */
class IcebergConcurrentWriteTest {

    @TempDir
    static Path tempDir;

    private static SparkSession spark;
    private static String table;

    @BeforeAll
    static void setUp() {
        System.setProperty("platform.warehouse", tempDir.resolve("warehouse").toString());
        System.setProperty("platform.catalog.type", "hadoop");
        System.setProperty("platform.database", "lake_concurrency_test");
        System.setProperty("platform.spark.shuffle.partitions", "2");
        PlatformConfig cfg = PlatformConfig.load();

        spark = SparkSessionFactory.create(cfg, "iceberg-concurrent-write-test");
        spark.sql("CREATE DATABASE IF NOT EXISTS " + cfg.catalogName() + "." + cfg.database());
        table = TableRef.of(cfg, "concurrent_target").fqn();
        spark.sql("CREATE TABLE " + table + " (id INT, note STRING) USING iceberg");
        spark.sql("INSERT INTO " + table + " VALUES (0, 'base')");
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void 两个写入者并发提交不会静默丢数据() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            // newSession() 共享 SparkContext 但给出独立的 SQL 会话，模拟两个writer的并发提交
            Callable<Void> writer1 = writeFromSession(spark.newSession(), 1);
            Callable<Void> writer2 = writeFromSession(spark.newSession(), 2);
            Future<Void> f1 = pool.submit(writer1);
            Future<Void> f2 = pool.submit(writer2);

            int succeeded = 0;
            Throwable firstError = null;
            for (Future<Void> f : new Future[]{f1, f2}) {
                try {
                    f.get(180, TimeUnit.SECONDS);
                    succeeded++;
                } catch (ExecutionException e) {
                    if (firstError == null) {
                        firstError = e.getCause();
                    }
                }
            }

            assertTrue(succeeded >= 1, "至少有一个写入者应成功: " + firstError);
            if (succeeded == 1) {
                assertTrue(hasCause(firstError, CommitFailedException.class),
                        "失败的那个应当是乐观锁冲突，而不是别的错误: " + firstError);
            }
            // 关键断言：成功的写入数 == 最终行数，没有「双方都说成功却少一行」的静默丢失。
            // 用新会话 + refreshTable：Iceberg 的 HadoopTableOperations 会缓存当前 metadata，
            // 老会话直接 count 可能还停在写入前的快照。
            spark.catalog().refreshTable(table);
            long rows = spark.newSession().table(table).count();
            assertEquals(1 + succeeded, rows,
                    "每个成功的写入者各加一行，并发提交不能丢数据");
        } finally {
            pool.shutdownNow();
        }
    }

    private static Callable<Void> writeFromSession(SparkSession session, int id) {
        return () -> {
            session.sql("INSERT INTO " + table + " VALUES (" + id + ", 'writer-" + id + "')");
            return null;
        };
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }
}
