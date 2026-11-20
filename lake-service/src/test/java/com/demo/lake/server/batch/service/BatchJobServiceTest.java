package com.demo.lake.server.batch.service;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.batch.model.RunsView;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.AuditBuffer;
import com.demo.lake.server.observability.AuditService;
import com.demo.lake.server.observability.LakeMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchJobServiceTest {

    private LakeProperties props(String template) {
        LakeProperties p = new LakeProperties();
        p.getBatch().setCommandTemplate(template);
        p.getBatch().setLogDir("target/test-logs");
        p.getBatch().setWorkDir(".");
        p.getBatch().setTimeoutSeconds(20);
        return p;
    }

    /** 单测不连 Trino：历史一律当读不到 */
    private BatchHistoryService noHistory() {
        return new BatchHistoryService(null, null) {
            @Override
            public BatchHistoryService.Result recent(int limit) {
                return new BatchHistoryService.Result(List.of(),
                        new RunsView.History(RunsView.History.NOT_INITIALIZED, "test", "iceberg.ops.batch_runs"));
            }

            @Override
            public BatchRun find(String id) {
                return null;
            }
        };
    }

    private InMemoryRunStateStore store;
    private InMemoryLogSink logs;
    private JobDispatcher dispatcher;
    private BatchIdentity identity;

    private BatchJobService newService(LakeProperties p) {
        store = new InMemoryRunStateStore();
        logs = new InMemoryLogSink();
        identity = new BatchIdentity();
        BatchHistoryService history = noHistory();
        dispatcher = new JobDispatcher(store, Map.of("local", new LocalProcessJobExecutor(p, logs)),
                new JobCompletionService(store, history, audit(), metrics()), identity, metrics(), p, Schedulers.immediate());
        return new BatchJobService(p, history, store, logs, identity, audit(), metrics(), Schedulers.immediate());
    }

    /** 测试里手动触发一次调度扫描（生产由 @PostConstruct 的定时器驱动） */
    private void dispatch() {
        dispatcher.dispatch();
    }

    private BatchRun await(BatchJobService service, String id) throws InterruptedException {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(20));
        while (Instant.now().isBefore(deadline)) {
            BatchRun run = service.get(id);
            if (!BatchRun.QUEUED.equals(run.status()) && !BatchRun.RUNNING.equals(run.status())) {
                return run;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("作业 20s 内没有结束: " + id);
    }

    @Test
    void 成功作业记录退出码与耗时() throws Exception {
        BatchJobService service = newService(props("/bin/sh -c \"echo done; exit 0\""));
        BatchRun submitted = service.submit(LakeJobs.INIT, null);
        assertEquals(BatchRun.QUEUED, submitted.status());
        dispatch();

        BatchRun done = await(service, submitted.id());
        assertEquals(BatchRun.SUCCEEDED, done.status());
        assertEquals(0, done.exitCode());
        assertTrue(done.durationMs() >= 0);
        assertTrue(service.tail(done.id(), 10).stream().anyMatch(l -> l.contains("done")));
    }

    @Test
    void 非零退出码标记失败() throws Exception {
        BatchJobService service = newService(props("/bin/sh -c \"exit 3\""));
        BatchRun submitted = service.submit(LakeJobs.INGEST, null);
        dispatch();
        BatchRun done = await(service, submitted.id());
        assertEquals(BatchRun.FAILED, done.status());
        assertEquals(3, done.exitCode());
        assertTrue(done.message().contains("3"));
    }

    @Test
    void 并发闸门让后来的作业排队() throws Exception {
        LakeProperties p = props("/bin/sh -c \"sleep 1; exit 0\"");
        p.getBatch().setMaxConcurrentJobs(1);
        BatchJobService service = newService(p);
        BatchRun first = service.submit(LakeJobs.INIT, null);
        BatchRun second = service.submit(LakeJobs.SHOW, null);
        dispatch();

        assertEquals(BatchRun.RUNNING, service.get(first.id()).status());
        assertEquals(BatchRun.QUEUED, service.get(second.id()).status());

        await(service, first.id());
        dispatch();
        assertEquals(BatchRun.SUCCEEDED, await(service, second.id()).status());
    }

    @Test
    void 幂等键重复提交返回同一条记录() {
        BatchJobService service = newService(props("/bin/sh -c \"exit 0\""));
        BatchRun a = service.submit(LakeJobs.INIT, "batch-2026-01");
        BatchRun b = service.submit(LakeJobs.INIT, "batch-2026-01");
        assertEquals(a.id(), b.id());
        assertEquals(1, store.recent(10).size());
        assertThrows(ResponseStatusException.class,
                () -> service.submit(LakeJobs.INIT, "bad key with spaces"));
    }

    @Test
    void 白名单外的作业直接拒绝() {
        BatchJobService service = newService(props("/bin/echo {job}"));
        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> service.submit("drop-everything", null));
        assertEquals(400, e.getStatusCode().value());
    }

    @Test
    void 关闭时拒绝所有作业() {
        LakeProperties p = props("/bin/echo {job}");
        p.getBatch().setEnabled(false);
        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> newService(p).submit(LakeJobs.SHOW, null));
        assertEquals(403, e.getStatusCode().value());
    }

    @Test
    void 列表合并湖表历史且运行状态优先() throws Exception {
        BatchRun persisted = new BatchRun("run-1", LakeJobs.INIT, "--job " + LakeJobs.INIT, BatchRun.SUCCEEDED, 0,
                Instant.parse("2026-11-01T00:00:00Z"), Instant.parse("2026-11-01T00:00:05Z"),
                5000L, null, "历史");
        BatchHistoryService history = new BatchHistoryService(null, null) {
            @Override
            public BatchHistoryService.Result recent(int limit) {
                return new BatchHistoryService.Result(List.of(persisted),
                        new RunsView.History(RunsView.History.OK, null, "iceberg.ops.batch_runs"));
            }

            @Override
            public BatchRun find(String id) {
                return "run-1".equals(id) ? persisted : null;
            }
        };
        LakeProperties p = props("/bin/sh -c \"echo done; exit 0\"");
        store = new InMemoryRunStateStore();
        logs = new InMemoryLogSink();
        identity = new BatchIdentity();
        dispatcher = new JobDispatcher(store, Map.of("local", new LocalProcessJobExecutor(p, logs)),
                new JobCompletionService(store, history, audit(), metrics()), identity, metrics(), p, Schedulers.immediate());
        BatchJobService service = new BatchJobService(p, history, store, logs, identity, audit(), metrics(), Schedulers.immediate());
        BatchRun submitted = service.submit(LakeJobs.INIT, null);
        dispatch();
        BatchRun done = await(service, submitted.id());

        assertEquals(2, service.list(20).runs().size());
        assertEquals(done, service.get(done.id()));
        assertEquals(persisted, service.get("run-1"));
        assertNotEquals(done.id(), persisted.id());
        assertTrue(service.tail("run-1", 10).isEmpty());
        assertThrows(ResponseStatusException.class, () -> service.get("run-unknown"));
    }

    @Test
    void 命令分词支持引号包裹的参数() {
        assertEquals(List.of("spark-submit", "--conf", "spark.driver.memory=2g", "--class", "a.B"),
                BatchJobService.tokenize("spark-submit --conf spark.driver.memory=2g --class a.B"));
        assertEquals(List.of("/bin/sh", "-c", "echo hello world"),
                BatchJobService.tokenize("/bin/sh -c \"echo hello world\""));
    }

    private static AuditService audit() {
        return new AuditService(new AuditBuffer());
    }

    private static LakeMetrics metrics() {
        return new LakeMetrics(new SimpleMeterRegistry());
    }

    @Test
    void tail优先读共享日志再退回本地文件() throws Exception {
        BatchJobService service = newService(props("/bin/echo never-dispatched"));
        BatchRun run = service.submit(LakeJobs.INIT, null);
        // 共享库有新行，本地文件故意写成不同内容
        logs.append(run.id(), List.of("shared-1", "shared-2"));
        Files.writeString(Path.of("target/test-logs", run.id() + ".log"), "local-only\n");

        List<String> tail = service.tail(run.id(), 10);
        assertEquals(List.of("shared-1", "shared-2"), tail);
    }

    /** 内存版共享日志，用于验证 tail / watcher 的读取顺序 */
    static class InMemoryLogSink implements LogSink {
        private final Map<String, List<LogLine>> lines = new ConcurrentHashMap<>();

        @Override
        public synchronized void append(String runId, List<String> newLines) {
            List<LogLine> all = lines.computeIfAbsent(runId, k -> new ArrayList<>());
            long seq = all.isEmpty() ? 0 : all.get(all.size() - 1).seq();
            for (String line : newLines) {
                all.add(new LogLine(++seq, line));
            }
        }

        @Override
        public List<LogLine> tail(String runId, int limit) {
            List<LogLine> all = lines.getOrDefault(runId, List.of());
            return List.copyOf(all.subList(Math.max(0, all.size() - limit), all.size()));
        }

        @Override
        public List<LogLine> since(String runId, long afterSeq, int limit) {
            return lines.getOrDefault(runId, List.of()).stream()
                    .filter(l -> l.seq() > afterSeq).limit(limit).toList();
        }

        @Override
        public int purge(Instant before) {
            return 0;
        }
    }

    /** 不连 Postgres 的测试替身：行为和 JdbcRunStateStore 一致，只是放在内存里 */
    static class InMemoryRunStateStore implements RunStateStore {
        private final Map<String, BatchRun> runs = new ConcurrentHashMap<>();
        private final Map<String, String> idempotency = new ConcurrentHashMap<>();

        @Override
        public void insert(BatchRun run, String owner) {
            runs.put(run.id(), run);
        }

        @Override
        public void finish(BatchRun run) {
            runs.put(run.id(), run);
        }

        @Override
        public Optional<BatchRun> find(String id) {
            return Optional.ofNullable(runs.get(id));
        }

        @Override
        public List<BatchRun> recent(int limit) {
            return runs.values().stream()
                    .sorted(Comparator.comparing(BatchRun::startedAt,
                            Comparator.nullsLast(Comparator.<Instant>reverseOrder())))
                    .limit(limit).toList();
        }

        @Override
        public void heartbeat(String owner, Instant now) {
        }

        @Override
        public List<BatchRun> staleRunning(Instant before) {
            return List.of();
        }

        @Override
        public void markLost(String id, String message, Instant now) {
            BatchRun run = runs.get(id);
            if (run != null) {
                runs.put(id, run.finished(BatchRun.FAILED, 1, message, 0));
            }
        }

        @Override
        public Optional<BatchRun> findByIdempotencyKey(String key) {
            String id = idempotency.get(key);
            return id == null ? Optional.empty() : find(id);
        }

        @Override
        public void recordIdempotency(String key, String runId) {
            idempotency.putIfAbsent(key, runId);
        }

        @Override
        public int runningCount() {
            return (int) runs.values().stream().filter(r -> BatchRun.RUNNING.equals(r.status())).count();
        }

        @Override
        public int queuedCount() {
            return (int) runs.values().stream().filter(r -> BatchRun.QUEUED.equals(r.status())).count();
        }

        @Override
        public BatchRun claimNext(String owner) {
            BatchRun next = runs.values().stream()
                    .filter(r -> BatchRun.QUEUED.equals(r.status()))
                    .min(Comparator.comparing(BatchRun::startedAt)).orElse(null);
            if (next == null) {
                return null;
            }
            BatchRun claimed = new BatchRun(next.id(), next.job(), next.command(), BatchRun.RUNNING, null,
                    next.startedAt(), null, null, next.logFile(), "执行中");
            runs.put(next.id(), claimed);
            return claimed;
        }

        @Override
        public void setHandle(String runId, String handle) {
        }
    }
}
