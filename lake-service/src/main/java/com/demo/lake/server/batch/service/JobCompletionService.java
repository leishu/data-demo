package com.demo.lake.server.batch.service;

import com.demo.lake.common.batch.BatchRunStatus;
import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.batch.store.RunStateStore;
import com.demo.lake.server.observability.AuditService;
import com.demo.lake.server.observability.LakeMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** 把执行器回调统一落成运行态终态，并让湖表历史缓存失效 */
@Component
@Slf4j
@RequiredArgsConstructor
public class JobCompletionService implements JobOutcomeListener {

    private final RunStateStore state;
    private final RunningRunRegistry liveRuns;
    private final BatchHistoryService history;
    private final AuditService audit;
    private final LakeMetrics metrics;

    @Override
    public void completed(BatchRun run, BatchRunStatus status, Integer exitCode, long elapsedMs, String message) {
        // 先摘心跳：终态写库失败时也要让对账接管，而不是留下永久 RUNNING
        liveRuns.remove(run.id());
        BatchRun done = run.finished(status, exitCode, message, elapsedMs);
        try {
            state.finish(done);
        } catch (RuntimeException e) {
            // 心跳对账兜底：终态写不进去也不能让作业一直挂在 RUNNING
            log.error("运行态终态写入失败 {}", run.id(), e);
        }
        history.invalidate();
        metrics.batchCompleted(status, elapsedMs);
        audit.record("batch.completed", "batch-service", java.util.Map.of(
                "run_id", run.id(), "job", run.job(), "status", status.name(),
                "duration_ms", String.valueOf(elapsedMs)));
        log.info("批作业 {} {} -> {}（{}）({}ms)", run.job(), run.id(), status, status.description(), elapsedMs);
    }
}
