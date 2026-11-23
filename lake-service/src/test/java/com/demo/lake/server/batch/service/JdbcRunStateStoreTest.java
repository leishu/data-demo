package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.model.BatchRun;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 用 H2 验证运行态表的 SQL 行为；不依赖 infra 里的 Postgres */
class JdbcRunStateStoreTest {

    private JdbcRunStateStore store;
    private JdbcDataSource ds;

    @BeforeEach
    void setUp() {
        ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:state-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        store = new JdbcRunStateStore(new JdbcTemplate(ds),
                new TransactionTemplate(new DataSourceTransactionManager(ds)));
        store.init();
    }

    private static BatchRun running(String id, Instant started) {
        return new BatchRun(id, "init", "--job init", BatchRun.RUNNING, null,
                started, null, null, "/tmp/" + id + ".log", "已提交");
    }

    @Test
    void insert查找与终态更新() throws Exception {
        Instant started = Instant.parse("2026-01-02T03:04:05Z");
        store.insert(running("r1", started), "svc-a");
        assertTrue(store.find("r1").isPresent());
        assertEquals(BatchRun.RUNNING, store.find("r1").orElseThrow().status());

        Instant finished = started.plusSeconds(5);
        store.finish(new BatchRun("r1", "init", "--job init", BatchRun.SUCCEEDED, 0,
                started, finished, 5000L, "/tmp/r1.log", "完成"));
        BatchRun done = store.find("r1").orElseThrow();
        assertEquals(BatchRun.SUCCEEDED, done.status());
        assertEquals(0, done.exitCode());
        assertEquals(finished, done.finishedAt());
        assertEquals(5000L, done.durationMs());
    }

    @Test
    void 心跳只续本实例的RUNNING并识别失联() throws Exception {
        store.insert(running("own", Instant.now().minusSeconds(600)), "svc-a");
        store.insert(running("other", Instant.now().minusSeconds(600)), "svc-b");

        // 模拟心跳停了 10 分钟（insert 默认写的是当前时间）
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.executeUpdate("UPDATE lake_batch.batch_runs SET heartbeat_ms="
                    + (System.currentTimeMillis() - 600_000));
        }
        assertEquals(2, store.staleRunning(Instant.now().minusSeconds(120)).size());

        store.heartbeat("own", Instant.now());
        assertEquals(1, store.staleRunning(Instant.now().minusSeconds(120)).size());
        try (Connection c = open(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT heartbeat_ms FROM lake_batch.batch_runs WHERE run_id='own'")) {
            assertTrue(rs.next());
            assertTrue(rs.getLong(1) > System.currentTimeMillis() - 10_000);
        }

        store.markLost("other", "实例失联", Instant.now());
        assertEquals(BatchRun.FAILED, store.find("other").orElseThrow().status());
        assertTrue(store.find("other").orElseThrow().message().contains("失联"));
    }

    @Test
    void recent按开始时间倒序并限量() {
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        for (int i = 0; i < 5; i++) {
            store.insert(running("r" + i, base.plusSeconds(i)), "svc-a");
        }
        List<BatchRun> latest = store.recent(3);
        assertEquals(List.of("r4", "r3", "r2"), latest.stream().map(BatchRun::id).toList());
    }

    @Test
    void 幂等键与排队认领() {
        BatchRun queued = new BatchRun("q1", "init", "--job init", BatchRun.QUEUED, null,
                Instant.now(), null, null, null, "已排队");
        store.insert(queued, "svc-a");
        store.recordIdempotency("key-1", "q1");
        store.recordIdempotency("key-1", "another"); // 重复写入应被忽略
        assertEquals("q1", store.findByIdempotencyKey("key-1").orElseThrow().id());

        assertEquals(0, store.runningCount());
        BatchRun claimed = store.claimNext("svc-b");
        assertEquals("q1", claimed.id());
        assertEquals(BatchRun.RUNNING, claimed.status());
        assertEquals(1, store.runningCount());
        assertNull(store.claimNext("svc-b"), "没有更多 QUEUED 时应返回 null");

        store.setHandle("q1", "pid:123");
        assertTrue(claimed.job().equals("init"));
    }

    @Test
    void 并发幂等提交只有一个胜出() throws Exception {
        Instant now = Instant.now();
        BatchRun a = queued("qa", now);
        BatchRun b = queued("qb", now.plusMillis(1));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<String> fa = pool.submit(() -> {
                start.await();
                return store.insertWithIdempotency(a, "svc-a", "same-key");
            });
            Future<String> fb = pool.submit(() -> {
                start.await();
                return store.insertWithIdempotency(b, "svc-b", "same-key");
            });
            start.countDown();
            String winnerA = fa.get(5, TimeUnit.SECONDS);
            String winnerB = fb.get(5, TimeUnit.SECONDS);
            assertEquals(winnerA, winnerB);
            assertEquals(1, store.recent(10).size(), "幂等命中不应插入第二条 run");
            assertEquals(winnerA, store.findByIdempotencyKey("same-key").orElseThrow().id());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void 失联终态保留耗时() {
        Instant started = Instant.now().minusSeconds(30);
        store.insert(running("lost", started), "svc-a");
        store.markLost("lost", "实例失联", started.plusSeconds(10));
        BatchRun lost = store.find("lost").orElseThrow();
        assertEquals(BatchRun.FAILED, lost.status());
        assertEquals(10_000L, lost.durationMs());
    }

    private static BatchRun queued(String id, Instant started) {
        return new BatchRun(id, "init", "--job init", BatchRun.QUEUED, null,
                started, null, null, null, "已排队");
    }

    private Connection open() throws Exception {
        return ds.getConnection();
    }
}
