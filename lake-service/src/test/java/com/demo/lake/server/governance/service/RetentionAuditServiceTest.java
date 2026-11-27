package com.demo.lake.server.governance.service;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.governance.model.RetentionAuditView;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.service.QueryService;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetentionAuditServiceTest {

    private static final QueryResult ROWS = new QueryResult(List.of(), List.of(
            List.of("run-ret", "retention.done", "bronze_orders", "false", "1", "12",
                    "1700000000000", "2026-11-15(12)")), 1, false, 0, null);

    private RetentionAuditService service(FakeQueries queries) {
        return new RetentionAuditService(queries, PlatformConfig.load(), Schedulers.immediate());
    }

    @Test
    void 审计事件映射正确() {
        RetentionAuditView view = service(new FakeQueries(ROWS)).recent(50).block().get(0);

        assertEquals("run-ret", view.runId());
        assertEquals("retention.done", view.event());
        assertEquals("bronze_orders", view.table());
        assertFalse(view.dryRun());
        assertEquals(1, view.partitions());
        assertEquals(12L, view.rowCount());
        assertEquals(1700000000000L, view.auditedMs());
        assertEquals("2026-11-15(12)", view.detail());
    }

    @Test
    void 查询的是保留审计表且失败时降级为空() {
        FakeQueries queries = new FakeQueries(ROWS);
        assertEquals(1, service(queries).recent(500).block().size());
        assertTrue(queries.lastSql.contains("ops.retention_audit"), queries.lastSql);
        assertTrue(queries.lastSql.contains("LIMIT 100"), queries.lastSql);

        assertTrue(service(new FakeQueries(ROWS, true)).recent(50).block().isEmpty());
    }

    private static final class FakeQueries extends QueryService {
        private final QueryResult result;
        private final boolean fail;
        private String lastSql;

        FakeQueries(QueryResult result) {
            this(result, false);
        }

        FakeQueries(QueryResult result, boolean fail) {
            super(null, new LakeProperties(), PlatformConfig.load(), Schedulers.immediate(), null);
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
