package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.store.RunningRunRegistry;
import com.demo.lake.server.batch.model.BatchIdentity;
import com.demo.lake.common.batch.BatchRunStatus;
import com.demo.lake.server.batch.model.BatchEvent;
import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.batch.store.LogStore;
import com.demo.lake.server.config.LakeProperties;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** SSE 首帧共享日志为空时，后续出现的日志必须能被推给客户端（跨实例场景） */
class BatchWatcherTest {

    @Test
    void 首帧无共享日志后续出现时切换过去() throws Exception {
        LakeProperties properties = new LakeProperties();
        BatchJobServiceTest.InMemoryRunStateStore store = new BatchJobServiceTest.InMemoryRunStateStore();
        // 前两次 tail 都当空：一次是建立流时的首帧，一次是第一次轮询；之后才「出现」日志
        AtomicInteger tails = new AtomicInteger();
        BatchJobServiceTest.InMemoryLogStore lateStore = new BatchJobServiceTest.InMemoryLogStore() {
            @Override
            public List<LogLine> tail(String runId, int limit) {
                return tails.incrementAndGet() <= 2 ? List.of() : super.tail(runId, limit);
            }
        };
        lateStore.append("watch-1", List.of("late-line"));

        BatchRun run = new BatchRun("watch-1", "init", "--job init", BatchRunStatus.RUNNING, null,
                Instant.now(), null, null, null, "执行中"); // logFile=null：模拟请求落到非执行实例
        store.insert(run, "svc-a");

        BatchJobService jobs = new BatchJobService(properties, null, store, lateStore, new BatchIdentity(),
                new RunningRunRegistry(), null, null, null, Schedulers.boundedElastic());
        BatchWatcher watcher = new BatchWatcher(jobs, lateStore, Schedulers.boundedElastic());

        List<BatchEvent> events = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch gotLog = new CountDownLatch(1);
        Disposable subscription = watcher.watch("watch-1", 40).subscribe(event -> {
            events.add(event);
            if (event instanceof BatchEvent.Log) {
                gotLog.countDown();
            }
        });
        try {
            assertTrue(gotLog.await(5, TimeUnit.SECONDS), "5s 内没有推来日志事件");
            assertTrue(events.stream().anyMatch(event -> event instanceof BatchEvent.Log log
                    && log.lines().contains("late-line")), "应该切换到共享日志并推出晚到的行");
        } finally {
            subscription.dispose();
        }
    }

    @Test
    void 运行不存在时以404错误结束流() {
        LakeProperties properties = new LakeProperties();
        BatchJobServiceTest.InMemoryRunStateStore store = new BatchJobServiceTest.InMemoryRunStateStore();
        BatchJobServiceTest.InMemoryLogStore logStore = new BatchJobServiceTest.InMemoryLogStore();
        BatchJobService jobs = new BatchJobService(properties, null, store, logStore, new BatchIdentity(),
                new RunningRunRegistry(), null, null, null, Schedulers.boundedElastic());
        BatchWatcher watcher = new BatchWatcher(jobs, logStore, Schedulers.boundedElastic());

        assertThrows(ResponseStatusException.class,
                () -> watcher.watch("missing", 40).collectList().block(Duration.ofSeconds(5)));
    }

    @Test
    void 终态运行时发出End并结束流() {
        LakeProperties properties = new LakeProperties();
        BatchJobServiceTest.InMemoryRunStateStore store = new BatchJobServiceTest.InMemoryRunStateStore();
        BatchJobServiceTest.InMemoryLogStore logStore = new BatchJobServiceTest.InMemoryLogStore();
        logStore.append("watch-2", List.of("last-line"));
        BatchRun run = new BatchRun("watch-2", "init", "--job init", BatchRunStatus.SUCCEEDED, 0,
                Instant.now(), Instant.now(), 100L, null, "完成");
        store.insert(run, "svc-a");

        BatchJobService jobs = new BatchJobService(properties, null, store, logStore, new BatchIdentity(),
                new RunningRunRegistry(), null, null, null, Schedulers.boundedElastic());
        BatchWatcher watcher = new BatchWatcher(jobs, logStore, Schedulers.boundedElastic());

        List<BatchEvent> events = watcher.watch("watch-2", 40)
                .collectList().block(Duration.ofSeconds(5));

        assertNotNull(events, "终态流应在 5s 内结束");
        assertTrue(events.stream().anyMatch(e -> e instanceof BatchEvent.Status), "应发出状态事件");
        assertTrue(events.stream().anyMatch(e -> e instanceof BatchEvent.Log log
                && log.lines().contains("last-line")), "应发出日志事件");
        assertTrue(events.stream().anyMatch(e -> e instanceof BatchEvent.End), "应发出 End 并结束");
    }
}
