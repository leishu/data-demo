package com.demo.lake.server.governance.service;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.governance.model.LineageView;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.service.QueryService;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineageServiceTest {

    private static final QueryResult ROWS = new QueryResult(List.of(), List.of(
            List.of("run-1", "transform", "123", "1700000000000")), 1, false, 0, null);

    private LineageService service(FakeQueries queries) {
        return new LineageService(queries, PlatformConfig.load(), Schedulers.immediate());
    }

    @Test
    void 从gold反查上游列血缘与写入快照() {
        LineageView view = service(new FakeQueries(ROWS)).forTable(LakeTables.GOLD_DAILY_REVENUE, 50).block();

        assertEquals(LakeTables.GOLD_DAILY_REVENUE, view.table());
        assertEquals(Arrays.asList(LakeTables.SILVER_ORDERS, LakeTables.BRONZE_ORDERS), view.upstream());
        assertEquals(List.of(), view.downstream(), "gold 是末端，没有下游");
        assertFalse(view.columns().isEmpty());
        assertTrue(view.columns().stream().anyMatch(c -> "gmv".equals(c.targetColumn())));
        assertEquals(1, view.runs().size());
        LineageView.RunView run = view.runs().get(0);
        assertEquals("run-1", run.runId());
        assertEquals("transform", run.job());
        assertEquals("123", run.snapshotId());
        assertEquals(1700000000000L, run.recordedMs());
    }

    @Test
    void 未知表名走400而不是击穿SQL() {
        FakeQueries queries = new FakeQueries(ROWS);
        ResponseStatusException error = assertThrows(ResponseStatusException.class,
                () -> service(queries).forTable("gold_daily_revenue; drop table x", 10).block());
        assertEquals(HttpStatus.BAD_REQUEST.value(), error.getStatusCode().value());
        assertNull(queries.lastSql, "非法表名不应触发查询");
    }

    @Test
    void 读血缘表失败时声明部分仍然可用() {
        LineageView view = service(new FakeQueries(ROWS, true)).forTable(LakeTables.BRONZE_ORDERS, 10).block();
        assertEquals(List.of(), view.runs());
        assertEquals(List.of(LakeTables.SILVER_ORDERS, LakeTables.GOLD_DAILY_REVENUE), view.downstream());
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
