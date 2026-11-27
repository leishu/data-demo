package com.demo.lake.server.query.service;

import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.service.LakeMetrics;
import com.demo.lake.server.query.QueryQuotaExceededException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import reactor.core.scheduler.Schedulers;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 配额：单账号限流与全局并发名额（状态放 H2，验证的是真实的跨实例 SQL 行为） */
class QuotaHelperTest {

    private JdbcDataSource ds;

    private QuotaHelper service(int concurrent, int perMinute, int timeoutMs) {
        return service(concurrent, perMinute, timeoutMs, 300);
    }

    private QuotaHelper service(int concurrent, int perMinute, int timeoutMs, int pollMs) {
        LakeProperties props = new LakeProperties();
        props.getQuery().setMaxConcurrentQueries(concurrent);
        props.getQuery().setMaxQueriesPerMinute(perMinute);
        props.getQuery().setAcquireTimeoutMs(timeoutMs);
        props.getQuery().setQuotaFallbackPollMs(pollMs);
        ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:quota-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        QuotaHelper quotas = new QuotaHelper(props, Schedulers.immediate(),
                new LakeMetrics(new SimpleMeterRegistry()),
                new JdbcTemplate(ds), new TransactionTemplate(new DataSourceTransactionManager(ds)));
        quotas.init();
        return quotas;
    }

    @Test
    void 单账号每分钟超限而其它账号不受影响() {
        QuotaHelper quotas = service(4, 2, 10);
        quotas.acquire("viewer").block();
        quotas.acquire("viewer").block();
        assertThrows(QueryQuotaExceededException.class, () -> quotas.acquire("viewer").block());
        assertDoesNotThrow(() -> quotas.acquire("operator").block());
    }

    @Test
    void 并发名额耗尽时超时并归还后可复用() {
        QuotaHelper quotas = service(1, 10, 30);
        QuotaHelper.Permit held = quotas.acquire("a").block();
        assertThrows(QueryQuotaExceededException.class, () -> quotas.acquire("b").block());
        held.close();
        // close 幂等：再关一次不能多归还名额，否则并发上限形同虚设
        held.close();
        assertDoesNotThrow(() -> quotas.acquire("c").block());
        assertThrows(QueryQuotaExceededException.class, () -> quotas.acquire("d").block());
    }

    @Test
    void 实例崩溃留下的租约到期后自动回收() throws Exception {
        QuotaHelper quotas = service(1, 10, 30);
        // 模拟实例被 kill：拿到名额后不调用 close，租约留在库里
        quotas.acquire("crashed").block();
        assertThrows(QueryQuotaExceededException.class, () -> quotas.acquire("waiter").block());

        // 等价于等到租约 TTL：把过期时间拨到过去，下一次 acquire 会顺手回收
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("UPDATE lake_batch.query_leases SET expires_ms = 0");
        }
        assertDoesNotThrow(() -> quotas.acquire("waiter").block());
    }

    @Test
    void 一次通知只唤醒一个等待者() {
        QuotaHelper quotas = service(1, 10, 1000);
        List<Integer> woke = Collections.synchronizedList(new ArrayList<>());
        quotas.registerWaiter().subscribe(v -> woke.add(1));
        quotas.registerWaiter().subscribe(v -> woke.add(2));

        assertEquals(2, quotas.pendingWaiters());
        quotas.wakeOne();
        assertEquals(1, woke.size());
        quotas.wakeAll();
        assertEquals(2, woke.size());
        assertEquals(0, quotas.pendingWaiters());
    }

    @Test
    void 通知唤醒可替代轮询且不需要等满超时() {
        // 兜底轮询间隔设得比 deadline 还长：没有通知就一定会超时
        QuotaHelper quotas = service(1, 10, 2000, 10_000);
        QuotaHelper.Permit held = quotas.acquire("a").block();
        // H2 本来走轮询；这里强制切到 LISTEN 唤醒方式，验证主路径的「通知替代轮询」
        quotas.setListening();

        AtomicReference<QuotaHelper.Permit> got = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();
        // Schedulers.immediate()：tryOnce + 登记等待者在 subscribe 调用内同步完成
        quotas.acquire("b").doOnSuccess(got::set).subscribe(v -> {}, err::set);
        assertEquals(1, quotas.pendingWaiters());

        held.close();   // 归还名额（H2 不发通知）
        quotas.wakeOne(); // 模拟 Postgres 的 pg_notify 到达

        assertNull(err.get());
        assertNotNull(got.get());
        assertEquals(0, quotas.pendingWaiters());
        got.get().close();
    }
}
