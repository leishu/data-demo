package com.demo.lake.server.observability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LakeMetricsTest {

    @Test
    void 关键SLI指标按名字注册并累计() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        LakeMetrics metrics = new LakeMetrics(registry);

        metrics.batchSubmitted("init");
        metrics.batchSubmitted("init");
        metrics.batchCompleted("SUCCEEDED", 1200);
        metrics.queueWait("init", 300);
        metrics.setQueueDepth(7);
        metrics.queryExecuted(true, 85, 12);
        metrics.quotaRejected("rate-limit");
        metrics.authDenied("insufficient-role");

        assertEquals(2.0, registry.get("lake.batch.submitted").tag("job", "init").counter().count());
        assertEquals(1.0, registry.get("lake.batch.completed").tag("status", "SUCCEEDED").counter().count());
        assertEquals(1200.0, registry.get("lake.batch.duration").timer().totalTime(java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(300.0, registry.get("lake.batch.queue.wait").timer().totalTime(java.util.concurrent.TimeUnit.MILLISECONDS));
        assertEquals(7.0, registry.get("lake.batch.queue.depth").gauge().value());
        assertEquals(1.0, registry.get("lake.query.executed").tag("outcome", "success").counter().count());
        assertEquals(12.0, registry.get("lake.query.rows").counter().count());
        assertEquals(1.0, registry.get("lake.query.quota.rejected").counter().count());
        assertEquals(1.0, registry.get("lake.auth.denied").counter().count());
    }
}
