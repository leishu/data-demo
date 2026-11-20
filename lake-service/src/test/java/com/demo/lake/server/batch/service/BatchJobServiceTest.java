package com.demo.lake.server.batch.service;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.batch.model.RunsView;
import com.demo.lake.server.config.LakeProperties;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    /** 单测不连 Trino：历史一律当读不到，只验内存那份 */
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

    private BatchJobService newService(LakeProperties p) {
        return new BatchJobService(p, noHistory(), new InMemoryRunStateStore(), Schedulers.immediate());
    }

    private BatchRun await(BatchJobService service, String id) throws InterruptedException {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(20));
        while (Instant.now().isBefore(deadline)) {
            BatchRun run = service.get(id);
            if (!BatchRun.RUNNING.equals(run.status())) {
                return run;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("作业 20s 内没有结束: " + id);
    }

    @Test
    void 成功作业记录退出码与耗时() throws Exception {
        BatchJobService service = newService(props("/bin/sh -c \"echo done; exit 0\""));
        BatchRun submitted = service.submit(LakeJobs.INIT);
        assertEquals(BatchRun.RUNNING, submitted.status());

        BatchRun done = await(service, submitted.id());
        assertEquals(BatchRun.SUCCEEDED, done.status());
        assertEquals(0, done.exitCode());
        assertTrue(done.durationMs() >= 0);
        assertTrue(service.tail(done.id(), 10).stream().anyMatch(l -> l.contains("done")));
    }

    @Test
    void 非零退出码标记失败() throws Exception {
        BatchJobService service = newService(props("/bin/sh -c \"exit 3\""));
        BatchRun done = await(service, service.submit(LakeJobs.INGEST).id());
        assertEquals(BatchRun.FAILED, done.status());
        assertEquals(3, done.exitCode());
        assertTrue(done.message().contains("3"));
    }

    @Test
    void 白名单外的作业直接拒绝() {
        BatchJobService service = newService(props("/bin/echo {job}"));
        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> service.submit("drop-everything"));
        assertEquals(400, e.getStatusCode().value());
    }

    @Test
    void 关闭时拒绝所有作业() {
        LakeProperties p = props("/bin/echo {job}");
        p.getBatch().setEnabled(false);
        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> newService(p).submit(LakeJobs.SHOW));
        assertEquals(403, e.getStatusCode().value());
    }

    @Test
    void 列表合并湖表历史且内存优先() throws Exception {
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
        BatchJobService service = new BatchJobService(props("/bin/sh -c \"echo done; exit 0\""), history,
                new InMemoryRunStateStore(), Schedulers.immediate());
        BatchRun done = await(service, service.submit(LakeJobs.INIT).id());

        assertEquals(2, service.list(20).runs().size());
        assertEquals(done, service.get(done.id()));
        assertEquals(persisted, service.get("run-1"));
        // 历史记录没有日志文件，tail 应该给空列表而不是 NPE
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

    /** 不连 Postgres 的测试替身：行为和 JdbcRunStateStore 一致，只是放在内存里 */
    static class InMemoryRunStateStore implements RunStateStore {
        private final Map<String, BatchRun> runs = new ConcurrentHashMap<>();

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
    }
}
