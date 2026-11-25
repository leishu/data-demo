package com.demo.lake.server.batch.service;

import com.demo.lake.common.batch.BatchRunStatus;
import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.batch.store.RunStateStore;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.service.LakeMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.Map;

/**
 * 调度器：从 Postgres 里按提交顺序认领 QUEUED 作业，受全局并发闸门约束。
 *
 * <p>认领是数据库里的原子 UPDATE（见 {@link RunStateStore#claimNext}），多实例之间不会重复执行。
 * 执行方式由 {@code lake.batch.executor} 选择（local / kubernetes），实现通过 bean 名注入。</p>
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class JobDispatcher {

    private final RunStateStore state;
    private final Map<String, JobExecutor> executors;
    private final JobCompletionService completion;
    private final BatchIdentity identity;
    private final RunningRunRegistry liveRuns;
    private final LakeMetrics metrics;
    private final LakeProperties props;
    private final Scheduler blocking;
    private final Disposable.Composite timer = Disposables.composite();

    @PostConstruct
    void start() {
        long interval = Math.max(200, props.getBatch().getDispatchIntervalMs());
        timer.add(Flux.interval(Duration.ofMillis(interval))
                .flatMap(tick -> Mono.fromCallable(() -> {
                    dispatch();
                    return true;
                }).onErrorResume(e -> {
                    log.warn("调度扫描失败: {}", e.getMessage());
                    return Mono.empty();
                }).subscribeOn(blocking))
                .subscribe());
        log.info("批作业调度器已启动: executor={} maxConcurrent={} interval={}ms",
                props.getBatch().getExecutor(), props.getBatch().getMaxConcurrentJobs(), interval);
    }

    void dispatch() {
        metrics.setQueueDepth(state.queuedCount());
        int max = Math.max(1, props.getBatch().getMaxConcurrentJobs());
        while (true) {
            // 名额检查与认领由状态库的全局行锁包住：多实例共享同一个并发上限
            BatchRun claimed = state.claimNextIfSlotAvailable(identity.id(), max);
            if (claimed == null) {
                return;
            }
            liveRuns.add(claimed.id());
            JobExecutor executor = executors.get(props.getBatch().getExecutor());
            if (executor == null) {
                completion.completed(claimed, BatchRunStatus.FAILED, 1, 0,
                        "未知执行器: " + props.getBatch().getExecutor());
                continue;
            }
            metrics.queueWait(claimed.job(), System.currentTimeMillis() - claimed.startedAt().toEpochMilli());
            try {
                String handle = executor.start(claimed, completion);
                state.setHandle(claimed.id(), handle);
                log.info("调度作业 {} -> {} handle={}", claimed.id(), claimed.job(), handle);
            } catch (RuntimeException e) {
                log.error("提交作业 {} 失败", claimed.id(), e);
                completion.completed(claimed, BatchRunStatus.FAILED, 1, 0, "提交失败: " + e.getMessage());
            }
        }
    }

    @PreDestroy
    void shutdown() {
        timer.dispose();
    }
}
