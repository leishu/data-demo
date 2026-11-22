package com.demo.lake.server.observability;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.config.LakeProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

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
    void 采集结果落到带table标签的gauge() {
        System.setProperty("platform.catalog.type", "hadoop");
        System.setProperty("platform.database", "lake_metrics_test");
        PlatformConfig cfg = PlatformConfig.load();

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        LakeProperties props = new LakeProperties();
        props.getMetrics().getIceberg().setTables(java.util.List.of("silver_orders"));
        IcebergTableMetrics collector = new IcebergTableMetrics(registry, cfg, props, null);
        collector.start();
        collector.stop(); // 不真的连 Trino（DataSource=null 会被 collect 吞掉），只验证 gauge 写入

        collector.update("silver_orders", 12, 4096.5, 49158, 3);
        assertEquals(12.0, registry.get("lake.iceberg.files").tag("table", "silver_orders").gauge().value());
        assertEquals(4096.5, registry.get("lake.iceberg.avg_file_size_bytes").tag("table", "silver_orders").gauge().value());
        assertEquals(49158.0, registry.get("lake.iceberg.total_size_bytes").tag("table", "silver_orders").gauge().value());
        assertEquals(3.0, registry.get("lake.iceberg.snapshots").tag("table", "silver_orders").gauge().value());
    }
}
