package com.demo.lake.server.query.service;

import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.LakeMetrics;
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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 配额：单账号限流与全局并发名额（状态放 H2，验证的是真实的跨实例 SQL 行为） */
class QueryQuotaServiceTest {

    private JdbcDataSource ds;

    private QueryQuotaService service(int concurrent, int perMinute, int timeoutMs) {
        LakeProperties props = new LakeProperties();
        props.getQuery().setMaxConcurrentQueries(concurrent);
        props.getQuery().setMaxQueriesPerMinute(perMinute);
        props.getQuery().setAcquireTimeoutMs(timeoutMs);
        ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:quota-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        QueryQuotaService quotas = new QueryQuotaService(props, Schedulers.immediate(),
                new LakeMetrics(new SimpleMeterRegistry()),
                new JdbcTemplate(ds), new TransactionTemplate(new DataSourceTransactionManager(ds)));
        quotas.init();
        return quotas;
    }

    @Test
    void 单账号每分钟超限而其它账号不受影响() {
        QueryQuotaService quotas = service(4, 2, 10);
        quotas.acquire("viewer").block();
        quotas.acquire("viewer").block();
        assertThrows(QueryQuotaExceededException.class, () -> quotas.acquire("viewer").block());
        assertDoesNotThrow(() -> quotas.acquire("operator").block());
    }

    @Test
    void 并发名额耗尽时超时并归还后可复用() {
        QueryQuotaService quotas = service(1, 10, 30);
        QueryQuotaService.Permit held = quotas.acquire("a").block();
        assertThrows(QueryQuotaExceededException.class, () -> quotas.acquire("b").block());
        held.close();
        // close 幂等：再关一次不能多归还名额，否则并发上限形同虚设
        held.close();
        assertDoesNotThrow(() -> quotas.acquire("c").block());
        assertThrows(QueryQuotaExceededException.class, () -> quotas.acquire("d").block());
    }

    @Test
    void 实例崩溃留下的租约到期后自动回收() throws Exception {
        QueryQuotaService quotas = service(1, 10, 30);
        // 模拟实例被 kill：拿到名额后不调用 close，租约留在库里
        quotas.acquire("crashed").block();
        assertThrows(QueryQuotaExceededException.class, () -> quotas.acquire("waiter").block());

        // 等价于等到租约 TTL：把过期时间拨到过去，下一次 acquire 会顺手回收
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("UPDATE lake_batch.query_leases SET expires_ms = 0");
        }
        assertDoesNotThrow(() -> quotas.acquire("waiter").block());
    }
}
