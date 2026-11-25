package com.demo.lake.server.observability.service;

import com.demo.lake.common.batch.BatchRunStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 业务 SLI 指标（进程内 Micrometer，经 /actuator/metrics 暴露；接 Prometheus 时直接抓这个名字）。
 * 名字和 SLO 目标一一对应，见 docs/OBSERVABILITY.md。
 */
@Component
public class LakeMetrics {

    private final MeterRegistry registry;
    private final AtomicInteger queueDepth = new AtomicInteger();

    public LakeMetrics(MeterRegistry registry) {
        this.registry = registry;
        registry.gauge("lake.batch.queue.depth", queueDepth);
    }

    public void batchSubmitted(String job) {
        registry.counter("lake.batch.submitted", "job", job == null ? "unknown" : job).increment();
    }

    public void batchCompleted(BatchRunStatus status, long durationMs) {
        String label = status == null ? "unknown" : status.name();
        registry.counter("lake.batch.completed", "status", label).increment();
        registry.timer("lake.batch.duration", "status", label)
                .record(Duration.ofMillis(Math.max(0, durationMs)));
    }

    public void queueWait(String job, long waitMs) {
        registry.timer("lake.batch.queue.wait", "job", job == null ? "unknown" : job)
                .record(Duration.ofMillis(Math.max(0, waitMs)));
    }

    public void setQueueDepth(int depth) {
        queueDepth.set(depth);
    }

    /** 一次用户查询的结果（成功/失败）与耗时；行数单独累加，便于看返回体量 */
    public void queryExecuted(boolean success, long elapsedMs, int rows) {
        registry.counter("lake.query.executed", "outcome", success ? "success" : "error").increment();
        registry.timer("lake.query.duration").record(Duration.ofMillis(Math.max(0, elapsedMs)));
        if (rows > 0) {
            registry.counter("lake.query.rows").increment(rows);
        }
    }

    public void quotaRejected(String reason) {
        registry.counter("lake.query.quota.rejected", "reason", reason).increment();
    }

    public void authDenied(String reason) {
        registry.counter("lake.auth.denied", "reason", reason).increment();
    }
}
