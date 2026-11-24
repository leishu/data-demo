package com.demo.lake.server.observability;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.config.LakeProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * 定时从 Iceberg 元数据表（$files / $snapshots）采集表级指标，补上 SLI 之外的「湖表健康度」。
 *
 * <p>走 Trino 只读连接池，不引 Iceberg 客户端：SQL 在 Trino 侧执行，服务只拿 4 个数字。
 * 表还不存在或 Trino 没起时保持上一次的 gauge 值，不影响服务启动或查询。</p>
 *
 * <p>指标名：lake.iceberg.files / avg_file_size_bytes / total_size_bytes / snapshots，都带 table 标签。</p>
 */
@Component
@Slf4j
public class IcebergTableMetrics {

    private static final Pattern SAFE_TABLE = Pattern.compile("[A-Za-z0-9_]+");
    /** 元数据表名要带 $ 且整个表名要加双引号，见 README 的 Trino 注意事项 */
    private static final String METADATA_TABLE = "%s.%s.\"%s$%s\"";
    private static final String FILES_SQL = """
            SELECT count(*) AS files, coalesce(avg(file_size_in_bytes), 0) AS avg_size,
            coalesce(sum(file_size_in_bytes), 0) AS total_size FROM %s
            """;
    private static final String SNAPSHOTS_SQL = "SELECT count(*) FROM %s";

    private final MeterRegistry registry;
    private final PlatformConfig cfg;
    private final LakeProperties props;
    private final JdbcTemplate jdbc;
    private final AtomicBoolean running = new AtomicBoolean();
    private final Map<String, TableGauges> gauges = new ConcurrentHashMap<>();
    private Thread worker;

    public IcebergTableMetrics(MeterRegistry registry, PlatformConfig cfg, LakeProperties props,
                               @Qualifier("trinoJdbc") JdbcTemplate jdbc) {
        this.registry = registry;
        this.cfg = cfg;
        this.props = props;
        this.jdbc = jdbc;
    }

    @PostConstruct
    void start() {
        LakeProperties.Metrics.Iceberg conf = props.getMetrics().getIceberg();
        if (!conf.isEnabled()) {
            return;
        }
        for (String table : conf.getTables()) {
            if (table == null || !SAFE_TABLE.matcher(table).matches()) {
                log.warn("跳过非法 Iceberg 表名: {}", table);
                continue;
            }
            gauges.put(table, new TableGauges(registry, table));
        }
        if (gauges.isEmpty()) {
            return;
        }
        running.set(true);
        worker = Thread.ofVirtual().name("iceberg-metrics").start(this::loop);
        log.info("Iceberg 表指标采集已启动: tables={} interval={}s", gauges.keySet(), conf.getIntervalSeconds());
    }

    @PreDestroy
    void stop() {
        running.set(false);
        if (worker != null) {
            worker.interrupt();
        }
    }

    private void loop() {
        long interval = Math.max(10, props.getMetrics().getIceberg().getIntervalSeconds()) * 1000L;
        while (running.get()) {
            for (String table : gauges.keySet()) {
                collect(table);
            }
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void collect(String table) {
        String filesSql = filesSql(cfg.catalogName(), cfg.database(), table);
        String snapshotsSql = snapshotsSql(cfg.catalogName(), cfg.database(), table);
        try {
            long files = 0, total = 0, snapshots = 0;
            double avg = 0;
            List<Object[]> filesRows = jdbc.query(filesSql, (rs, n) -> new Object[]{rs.getLong(1), rs.getDouble(2), rs.getLong(3)});
            if (!filesRows.isEmpty()) {
                Object[] row = filesRows.get(0);
                files = ((Number) row[0]).longValue();
                avg = ((Number) row[1]).doubleValue();
                total = ((Number) row[2]).longValue();
            }
            List<Long> snapshotRows = jdbc.query(snapshotsSql, (rs, n) -> rs.getLong(1));
            if (!snapshotRows.isEmpty()) {
                snapshots = snapshotRows.get(0);
            }
            update(table, files, avg, total, snapshots);
        } catch (DataAccessException e) {
            // 元数据表不存在 / Trino 未就绪都很正常，保留上次的值
            log.debug("采集 Iceberg 表指标失败 table={}: {}", table, e.getMessage());
        }
    }

    /** 包内可见，单测用：把一次采集结果写进 gauge */
    void update(String table, long files, double avgSizeBytes, long totalSizeBytes, long snapshots) {
        TableGauges g = gauges.get(table);
        if (g != null) {
            g.set(files, avgSizeBytes, totalSizeBytes, snapshots);
        }
    }

    /** 元数据表要带 $ 且整个表名要加双引号，见 README 的 Trino 注意事项 */
    static String metadataTable(String catalog, String schema, String table, String metadata) {
        return String.format(METADATA_TABLE, catalog, schema, table, metadata);
    }

    static String filesSql(String catalog, String schema, String table) {
        return String.format(FILES_SQL, metadataTable(catalog, schema, table, "files"));
    }

    static String snapshotsSql(String catalog, String schema, String table) {
        return String.format(SNAPSHOTS_SQL, metadataTable(catalog, schema, table, "snapshots"));
    }

    /** 每个表四个 Atomic 容器，注册一次 gauge，后续只改值 */
    private static final class TableGauges {
        private final AtomicLong files = new AtomicLong();
        private final AtomicReference<Double> avg = new AtomicReference<>(0.0);
        private final AtomicLong total = new AtomicLong();
        private final AtomicLong snapshots = new AtomicLong();

        TableGauges(MeterRegistry registry, String table) {
            Tags tags = Tags.of("table", table);
            registry.gauge("lake.iceberg.files", tags, files, AtomicLong::get);
            registry.gauge("lake.iceberg.avg_file_size_bytes", tags, avg, AtomicReference::get);
            registry.gauge("lake.iceberg.total_size_bytes", tags, total, AtomicLong::get);
            registry.gauge("lake.iceberg.snapshots", tags, snapshots, AtomicLong::get);
        }

        void set(long fileCount, double avgSize, long totalSize, long snapshotCount) {
            files.set(fileCount);
            avg.set(avgSize);
            total.set(totalSize);
            snapshots.set(snapshotCount);
        }
    }
}
