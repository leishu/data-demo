package com.demo.lake.server.observability.service;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.governance.model.DqResultView;
import com.demo.lake.server.governance.service.DqResultService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 指标换算与清零逻辑：Prometheus 规则只比数字，这里保证数字的含义不漂。 */
class GovernanceMetricsTest {

    private static IcebergTableMetrics icebergWith(Map<String, IcebergTableMetrics.TableSnapshot> snapshots) {
        return new IcebergTableMetrics(new SimpleMeterRegistry(), PlatformConfig.load(), new LakeProperties(),
                new JdbcTemplate(new JdbcDataSource())) {
            @Override
            public Map<String, IcebergTableMetrics.TableSnapshot> snapshot() {
                return snapshots;
            }
        };
    }

    private static DqResultService dqWith(List<DqResultView> rows) {
        return new DqResultService(null, null, null) {
            @Override
            public Mono<List<DqResultView>> recentStrict(int limit) {
                return Mono.just(rows);
            }
        };
    }

    @Test
    void 新鲜度按快照时间换算且SLA来自目录() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        long tenHoursAgo = System.currentTimeMillis() - 10 * 3_600_000L;
        Map<String, IcebergTableMetrics.TableSnapshot> snapshots = Map.of(
                LakeTables.SILVER_ORDERS,
                new IcebergTableMetrics.TableSnapshot(4, 4096, 16384, 3, tenHoursAgo));
        GovernanceMetrics metrics = new GovernanceMetrics(registry, icebergWith(snapshots),
                dqWith(List.of()), new LakeProperties());

        metrics.collect();

        assertEquals(26.0, registry.get(GovernanceMetrics.FRESHNESS_SLA_HOURS)
                .tag("table", LakeTables.SILVER_ORDERS).gauge().value());
        assertEquals(10.0, registry.get(GovernanceMetrics.FRESHNESS_HOURS)
                .tag("table", LakeTables.SILVER_ORDERS).gauge().value(), 0.5);
        // 没有快照的表补 -1，让 Prometheus 的 >= 0 条件把它当 UNKNOWN 跳过
        assertEquals(-1.0, registry.get(GovernanceMetrics.FRESHNESS_HOURS)
                .tag("table", LakeTables.GOLD_DAILY_REVENUE).gauge().value());
    }

    @Test
    void 质量失败按检查项出gauge且恢复后清零() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DqResultView failed = new DqResultView("run-1", "1", "silver_unique", "ERROR",
                12.0, 0.0, "重复订单号", 1L, false);
        DqResultView passed = new DqResultView("run-1", "1", "gold_net_revenue", "WARN",
                1.0, 1.0, "", 1L, true);

        class MutableDq extends DqResultService {
            private List<DqResultView> rows = List.of(failed, passed);

            MutableDq() {
                super(null, null, null);
            }

            void setRows(List<DqResultView> rows) {
                this.rows = rows;
            }

            @Override
            public Mono<List<DqResultView>> recentStrict(int limit) {
                return Mono.just(rows);
            }
        }
        MutableDq dq = new MutableDq();
        GovernanceMetrics metrics = new GovernanceMetrics(registry, icebergWith(Map.of()),
                dq, new LakeProperties());

        metrics.collect();
        assertEquals(1.0, registry.get(GovernanceMetrics.DQ_FAILED)
                .tag("check", "silver_unique").tag("severity", "ERROR").gauge().value());
        assertEquals(0.0, registry.get(GovernanceMetrics.DQ_FAILED)
                .tag("check", "gold_net_revenue").tag("severity", "WARN").gauge().value());

        dq.setRows(List.of(new DqResultView("run-2", "2", "silver_unique", "ERROR",
                0.0, 0.0, "", 2L, true)));
        metrics.collect();
        assertEquals(0.0, registry.get(GovernanceMetrics.DQ_FAILED)
                .tag("check", "silver_unique").tag("severity", "ERROR").gauge().value());
    }

    @Test
    void 质量结果读失败时保留上一轮告警() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DqResultView failed = new DqResultView("run-1", "1", "silver_unique", "ERROR",
                12.0, 0.0, "重复订单号", 1L, false);

        class FlakyDq extends DqResultService {
            private boolean down;

            FlakyDq() {
                super(null, null, null);
            }

            @Override
            public Mono<List<DqResultView>> recentStrict(int limit) {
                return down ? Mono.error(new RuntimeException("Trino 不可用"))
                        : Mono.just(List.of(failed));
            }
        }
        FlakyDq dq = new FlakyDq();
        GovernanceMetrics metrics = new GovernanceMetrics(registry, icebergWith(Map.of()),
                dq, new LakeProperties());

        metrics.collect();
        assertEquals(1.0, registry.get(GovernanceMetrics.DQ_FAILED)
                .tag("check", "silver_unique").tag("severity", "ERROR").gauge().value());

        dq.down = true;
        metrics.collect();
        assertEquals(1.0, registry.get(GovernanceMetrics.DQ_FAILED)
                .tag("check", "silver_unique").tag("severity", "ERROR").gauge().value(),
                "读失败不能当成恢复，否则告警会被静默清掉");
    }
}
