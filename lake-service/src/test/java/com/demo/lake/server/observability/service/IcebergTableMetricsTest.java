package com.demo.lake.server.observability.service;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.config.LakeProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergTableMetricsTest {

    @Test
    void 元数据表名带美元符号且加双引号() {
        String files = IcebergTableMetrics.filesSql("iceberg", "lake", "silver_orders");
        String snapshots = IcebergTableMetrics.snapshotsSql("iceberg", "lake", "silver_orders");

        assertTrue(files.contains("iceberg.lake.\"silver_orders$files\""), files);
        assertTrue(files.contains("file_size_in_bytes"), files);
        assertTrue(snapshots.contains("iceberg.lake.\"silver_orders$snapshots\""), snapshots);
    }

    @Test
    void 最近提交时间查的是snapshots且用unixtime避开驱动类型差异() {
        String sql = IcebergTableMetrics.lastCommitSql("iceberg", "lake", "silver_orders");
        assertTrue(sql.contains("iceberg.lake.\"silver_orders$snapshots\""), sql);
        assertTrue(sql.contains("to_unixtime(committed_at)"), sql);
    }

    @Test
    void 采集结果落到带table标签的gauge() {
        System.setProperty("platform.catalog.type", "hadoop");
        System.setProperty("platform.database", "lake_metrics_test");
        PlatformConfig cfg = PlatformConfig.load();

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        LakeProperties props = new LakeProperties();
        props.getMetrics().getIceberg().setTables(java.util.List.of("silver_orders"));
        // 用无元数据表的 H2 占位：collect 会抛 DataAccessException 被吞掉，只验证 gauge 写入
        IcebergTableMetrics collector = new IcebergTableMetrics(registry, cfg, props, new JdbcTemplate(new JdbcDataSource()));
        collector.start();
        collector.stop();

        collector.update("silver_orders", 12, 4096.5, 49158, 3, 1_700_000_000_000L);
        assertEquals(12.0, registry.get("lake.iceberg.files").tag("table", "silver_orders").gauge().value());
        assertEquals(4096.5, registry.get("lake.iceberg.avg_file_size_bytes").tag("table", "silver_orders").gauge().value());
        assertEquals(49158.0, registry.get("lake.iceberg.total_size_bytes").tag("table", "silver_orders").gauge().value());
        assertEquals(3.0, registry.get("lake.iceberg.snapshots").tag("table", "silver_orders").gauge().value());
        assertEquals(1_700_000_000_000.0,
                registry.get("lake.iceberg.last_committed_epoch_ms").tag("table", "silver_orders").gauge().value());
        assertEquals(1_700_000_000_000L, collector.snapshot().get("silver_orders").lastCommittedMs());
    }
}
