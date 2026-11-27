package com.demo.lake.server.governance.service;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.governance.model.GovernanceAsset;
import com.demo.lake.server.observability.service.IcebergTableMetrics;
import com.demo.lake.server.observability.service.IcebergTableMetrics.TableSnapshot;
import com.demo.lake.server.query.model.TableInfo;
import com.demo.lake.server.query.service.QueryService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GovernanceServiceTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final long HOUR = 3_600_000L;

    /** 只替掉指标采集结果：不连 Trino，也不依赖 update() 的包级可见性 */
    private IcebergTableMetrics iceberg(Map<String, TableSnapshot> snapshots) {
        System.setProperty("platform.catalog.type", "hadoop");
        System.setProperty("platform.database", "lake_governance_test");
        PlatformConfig cfg = PlatformConfig.load();
        return new IcebergTableMetrics(new SimpleMeterRegistry(), cfg, new LakeProperties(),
                new JdbcTemplate(new JdbcDataSource())) {
            @Override
            public Map<String, TableSnapshot> snapshot() {
                return snapshots;
            }
        };
    }

    private GovernanceService service(List<TableInfo> tables, IcebergTableMetrics iceberg) {
        return new GovernanceService(new FakeQueries(tables), iceberg);
    }

    private static GovernanceAsset byTable(List<GovernanceAsset> assets, String table) {
        return assets.stream().filter(a -> table.equals(a.table())).findFirst().orElseThrow();
    }

    @Test
    void 未建表的登记资产仍出现且标记为不存在() {
        List<GovernanceAsset> assets = service(List.of(), iceberg(Map.of())).assets(NOW).block();

        assertEquals(3, assets.size());
        GovernanceAsset bronze = byTable(assets, LakeTables.BRONZE_ORDERS);
        assertFalse(bronze.exists());
        assertTrue(bronze.registered());
        assertEquals("数据平台组", bronze.owner());
        assertEquals("内部", bronze.sensitivityLabel());
        assertEquals(2, bronze.sensitivityLevel());
        assertEquals(GovernanceAsset.Freshness.UNKNOWN, bronze.freshness().status());
        assertNull(bronze.freshness().lastUpdatedMs());
    }

    @Test
    void 未登记的表不会被丢掉() {
        List<TableInfo> tables = List.of(new TableInfo("lake", "mystery_table", "BASE TABLE", List.of()));
        List<GovernanceAsset> assets = service(tables, iceberg(Map.of())).assets(NOW).block();

        assertEquals(4, assets.size());
        GovernanceAsset mystery = byTable(assets, "mystery_table");
        assertTrue(mystery.exists());
        assertFalse(mystery.registered());
        assertNull(mystery.owner());
        assertEquals("未登记", mystery.sensitivityLabel());
        assertEquals(GovernanceAsset.Freshness.UNKNOWN, mystery.freshness().status());
    }

    @Test
    void 合并iceberg指标并按SLA判定新鲜度() {
        // silver 10 小时前更新（SLA 26h → OK），gold 60 小时前（> 2×26h → BREACH）
        Map<String, TableSnapshot> snapshots = Map.of(
                LakeTables.SILVER_ORDERS, new TableSnapshot(12, 4096.5, 49158, 3, NOW - 10 * HOUR),
                LakeTables.GOLD_DAILY_REVENUE, new TableSnapshot(4, 8192, 32768, 2, NOW - 60 * HOUR));
        List<TableInfo> tables = List.of(
                new TableInfo("lake", LakeTables.SILVER_ORDERS, "BASE TABLE", List.of()),
                new TableInfo("lake", LakeTables.GOLD_DAILY_REVENUE, "BASE TABLE", List.of()));
        List<GovernanceAsset> assets = service(tables, iceberg(snapshots)).assets(NOW).block();

        GovernanceAsset silver = byTable(assets, LakeTables.SILVER_ORDERS);
        assertEquals(GovernanceAsset.Freshness.OK, silver.freshness().status());
        assertEquals(10.0, silver.freshness().hoursSinceUpdate());
        assertEquals(12L, silver.metrics().fileCount());
        assertEquals(3L, silver.metrics().snapshotCount());

        GovernanceAsset gold = byTable(assets, LakeTables.GOLD_DAILY_REVENUE);
        assertEquals(GovernanceAsset.Freshness.BREACH, gold.freshness().status());
        assertEquals(60.0, gold.freshness().hoursSinceUpdate());
        assertEquals(32768L, gold.metrics().totalSizeBytes());
    }

    @Test
    void 介于SLA与两倍SLA之间判为警告() {
        Map<String, TableSnapshot> snapshots = Map.of(
                LakeTables.BRONZE_ORDERS, new TableSnapshot(1, 1024, 1024, 1, NOW - 40 * HOUR));
        List<GovernanceAsset> assets = service(List.of(), iceberg(snapshots)).assets(NOW).block();
        assertEquals(GovernanceAsset.Freshness.WARN, byTable(assets, LakeTables.BRONZE_ORDERS).freshness().status());
    }

    /** 不连 Trino：只替掉表清单这一条，其余走真实装配逻辑 */
    private static final class FakeQueries extends QueryService {
        private final List<TableInfo> tables;

        FakeQueries(List<TableInfo> tables) {
            super(null, new LakeProperties(), PlatformConfig.load(), Schedulers.immediate(), null);
            this.tables = tables;
        }

        @Override
        public Mono<List<TableInfo>> listTables() {
            return Mono.just(tables);
        }
    }
}
