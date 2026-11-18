package com.demo.lake.servier.batch.service;

import com.demo.lake.servier.batch.model.BatchRun;
import com.demo.lake.servier.config.LakeProperties;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

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
        BatchJobService service = new BatchJobService(props("/bin/sh -c \"echo done; exit 0\""));
        BatchRun submitted = service.submit("init");
        assertEquals(BatchRun.RUNNING, submitted.status());

        BatchRun done = await(service, submitted.id());
        assertEquals(BatchRun.SUCCEEDED, done.status());
        assertEquals(0, done.exitCode());
        assertTrue(done.durationMs() >= 0);
        assertTrue(service.tail(done.id(), 10).stream().anyMatch(l -> l.contains("done")));
    }

    @Test
    void 非零退出码标记失败() throws Exception {
        BatchJobService service = new BatchJobService(props("/bin/sh -c \"exit 3\""));
        BatchRun done = await(service, service.submit("ingest").id());
        assertEquals(BatchRun.FAILED, done.status());
        assertEquals(3, done.exitCode());
        assertTrue(done.message().contains("3"));
    }

    @Test
    void 白名单外的作业直接拒绝() {
        BatchJobService service = new BatchJobService(props("/bin/echo {job}"));
        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> service.submit("drop-everything"));
        assertEquals(400, e.getStatusCode().value());
    }

    @Test
    void 关闭时拒绝所有作业() {
        LakeProperties p = props("/bin/echo {job}");
        p.getBatch().setEnabled(false);
        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> new BatchJobService(p).submit("show"));
        assertEquals(403, e.getStatusCode().value());
    }

    @Test
    void 命令分词支持引号包裹的参数() {
        assertEquals(List.of("spark-submit", "--conf", "spark.driver.memory=2g", "--class", "a.B"),
                BatchJobService.tokenize("spark-submit --conf spark.driver.memory=2g --class a.B"));
        assertEquals(List.of("/bin/sh", "-c", "echo hello world"),
                BatchJobService.tokenize("/bin/sh -c \"echo hello world\""));
    }
}
