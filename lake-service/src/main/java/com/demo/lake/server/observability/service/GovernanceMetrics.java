package com.demo.lake.server.observability.service;

import com.demo.lake.common.governance.AssetDefinition;
import com.demo.lake.common.governance.GovernanceCatalog;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.governance.model.DqResultView;
import com.demo.lake.server.governance.service.DqResultService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 把「新鲜度 SLA 超时」和「质量检查失败」写成 Prometheus 可判定的 gauge。
 *
 * <p>数据不另起一套采集：新鲜度用 {@link IcebergTableMetrics#snapshot()}（已经在定时查 $snapshots），
 * 质量用 {@link DqResultService#recent(int)}（治理页同一份结果）。这里只做换算与打点，
 * 阈值由 Prometheus 规则比较 hours 与 sla.hours，不在服务里硬编码「告警还是不告警」。</p>
 *
 * <p>没有快照时 hours 补 -1（不是「现在」，也不是 0），规则里的 {@code >= 0} 会跳过 UNKNOWN，
 * 避免表还没建就误报超时。</p>
 */
@Component
@Slf4j
public class GovernanceMetrics {

    static final String FRESHNESS_HOURS = "lake.governance.freshness.hours";
    static final String FRESHNESS_SLA_HOURS = "lake.governance.freshness.sla.hours";
    static final String DQ_FAILED = "lake.governance.dq.failed";

    private static final int MAX_DQ_RESULTS = 100;
    private static final long HOUR_MS = 3_600_000L;

    private final MeterRegistry registry;
    private final IcebergTableMetrics iceberg;
    private final DqResultService dq;
    private final LakeProperties props;

    // gauge 值持有者要一直留着，下次只改值；Micrometer 不提供「按标签更新已注册 gauge」的接口
    private final Map<Tags, AtomicLong> freshness = new ConcurrentHashMap<>();
    private final Map<Tags, AtomicLong> sla = new ConcurrentHashMap<>();
    private final Map<Tags, AtomicLong> dqFailed = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread worker;

    public GovernanceMetrics(MeterRegistry registry, IcebergTableMetrics iceberg, DqResultService dq,
                             LakeProperties props) {
        this.registry = registry;
        this.iceberg = iceberg;
        this.dq = dq;
        this.props = props;
    }

    @PostConstruct
    void start() {
        if (!props.getMetrics().getGovernance().isEnabled()) {
            return;
        }
        running.set(true);
        worker = Thread.ofVirtual().name("governance-metrics").start(this::loop);
        log.info("治理告警指标已启动: interval={}s", props.getMetrics().getGovernance().getIntervalSeconds());
    }

    @PreDestroy
    void stop() {
        running.set(false);
        if (worker != null) {
            worker.interrupt();
        }
    }

    private void loop() {
        long interval = Math.max(10, props.getMetrics().getGovernance().getIntervalSeconds()) * 1000L;
        while (running.get()) {
            collect();
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** 一次采集；包内可见给单测直接调用，不必启动后台线程 */
    void collect() {
        collectFreshness();
        collectQuality();
    }

    private void collectFreshness() {
        Map<String, IcebergTableMetrics.TableSnapshot> snapshots = iceberg.snapshot();
        long now = System.currentTimeMillis();
        for (AssetDefinition asset : GovernanceCatalog.ASSETS) {
            Tags tags = Tags.of("table", asset.getTable());
            gauge(FRESHNESS_SLA_HOURS, tags, sla).set(asset.getFreshnessSlaHours());
            IcebergTableMetrics.TableSnapshot snapshot = snapshots.get(asset.getTable());
            long hours = -1;
            if (snapshot != null && snapshot.lastCommittedMs() > 0) {
                hours = Math.max(0, (now - snapshot.lastCommittedMs()) / HOUR_MS);
            }
            gauge(FRESHNESS_HOURS, tags, freshness).set(hours);
        }
    }

    private void collectQuality() {
        // 先清零：上一次失败、这次恢复的检查必须回到 0，否则告警永远不会消
        dqFailed.values().forEach(v -> v.set(0));
        List<DqResultView> rows;
        try {
            rows = dq.recent(MAX_DQ_RESULTS).block(Duration.ofSeconds(10));
        } catch (RuntimeException e) {
            log.debug("采集质量检查指标失败: {}", e.getMessage());
            return;
        }
        if (rows == null) {
            return;
        }
        // 同一检查可能有多轮运行，治理页按时间倒序返回，这里只认最新一条
        Map<String, DqResultView> latest = new LinkedHashMap<>();
        for (DqResultView row : rows) {
            if (row.check() != null) {
                latest.putIfAbsent(row.check(), row);
            }
        }
        for (DqResultView row : latest.values()) {
            Double observed = row.observed();
            Double threshold = row.threshold();
            if (observed == null || threshold == null) {
                continue;
            }
            Tags tags = Tags.of("check", safe(row.check()), "severity", safe(row.severity()));
            gauge(DQ_FAILED, tags, dqFailed).set(observed > threshold ? 1 : 0);
        }
    }

    private AtomicLong gauge(String name, Tags tags, Map<Tags, AtomicLong> values) {
        return values.computeIfAbsent(tags, t -> {
            AtomicLong value = new AtomicLong();
            registry.gauge(name, t, value, AtomicLong::get);
            return value;
        });
    }

    private static String safe(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }
}
