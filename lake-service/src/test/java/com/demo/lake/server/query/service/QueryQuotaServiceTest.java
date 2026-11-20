package com.demo.lake.server.query.service;

import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.query.QueryQuotaExceededException;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 配额：单账号限流与全局并发名额（不连 Trino，纯内存行为） */
class QueryQuotaServiceTest {

    private QueryQuotaService service(int concurrent, int perMinute, int timeoutMs) {
        LakeProperties props = new LakeProperties();
        props.getQuery().setMaxConcurrentQueries(concurrent);
        props.getQuery().setMaxQueriesPerMinute(perMinute);
        props.getQuery().setAcquireTimeoutMs(timeoutMs);
        return new QueryQuotaService(props, Schedulers.immediate());
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
}
