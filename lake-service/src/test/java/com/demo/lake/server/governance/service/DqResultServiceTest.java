package com.demo.lake.server.governance.service;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.governance.model.DqResultView;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.service.QueryService;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqResultServiceTest {

    private static QueryResult result() {
        return new QueryResult(List.of(), List.of(
                List.of("run-1", "123", "silver_unique", "ERROR", "1.0", "0.0", "DQ-DUP", "1700000000000"),
                List.of("run-1", "123", "gold_refund_rate", "ERROR", "0.5", "1.0", "", "1700000000001")),
                2, false, 0, null);
    }

    private static FakeQueries fake(boolean fail) {
        return new FakeQueries(result(), fail);
    }

    private DqResultService service(FakeQueries queries) {
        return new DqResultService(queries, PlatformConfig.load(), Schedulers.immediate());
    }

    @Test
    void 结果映射并现算通过与否() {
        List<DqResultView> views = service(fake(false)).recent(50).block();

        assertEquals(2, views.size());
        DqResultView failed = views.get(0);
        assertEquals("run-1", failed.runId());
        assertEquals("123", failed.snapshotId());
        assertEquals("silver_unique", failed.check());
        assertEquals("ERROR", failed.severity());
        assertEquals(1.0, failed.observed());
        assertEquals("DQ-DUP", failed.detail());
        assertEquals(1700000000000L, failed.checkedMs());
        assertFalse(failed.passed());
        assertTrue(views.get(1).passed());
    }

    @Test
    void 查询读到的是ops表且check列被双引号包住() {
        FakeQueries queries = fake(false);
        service(queries).recent(500).block();

        assertTrue(queries.lastSql.contains("ops.dq_results"), queries.lastSql);
        assertTrue(queries.lastSql.contains("\"check\""), queries.lastSql);
        assertTrue(queries.lastSql.contains("LIMIT 100"), queries.lastSql);
    }

    @Test
    void 表不存在或查询失败时降级为空列表() {
        assertTrue(service(fake(true)).recent(50).block().isEmpty());
    }

    /** 不连 Trino：只替掉 executeQuiet，验证装配与降级 */
    private static final class FakeQueries extends QueryService {
        private final QueryResult result;
        private final boolean fail;
        private String lastSql;

        FakeQueries(QueryResult result, boolean fail) {
            super(null, new LakeProperties(), PlatformConfig.load(), Schedulers.immediate());
            this.result = result;
            this.fail = fail;
        }

        @Override
        public QueryResult executeQuiet(String sql, Integer maxRows) {
            return answer(sql);
        }

        @Override
        public QueryResult executeQuiet(String sql, Integer maxRows, Object... params) {
            return answer(sql);
        }

        private QueryResult answer(String sql) {
            lastSql = sql;
            if (fail) {
                throw new IllegalStateException("表不存在");
            }
            return result;
        }
    }
}
