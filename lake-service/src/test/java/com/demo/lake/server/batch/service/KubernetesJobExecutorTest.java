package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.config.LakeProperties;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 用 JDK 自带的 HttpServer 假扮 K8s API，验证提交、终态轮询与 Pod 日志搬运，不依赖真实集群 */
class KubernetesJobExecutorTest {

    @Test
    void 创建Job并轮询到成功后回调() throws Exception {
        AtomicReference<String> created = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String prefix = "/apis/batch/v1/namespaces/default/jobs";
        server.createContext(prefix, exchange -> {
            created.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] body = "{\"kind\":\"Job\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext(prefix + "/lake-run-k8s1", exchange -> {
            byte[] body = "{\"status\":{\"succeeded\":1}}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            LakeProperties props = new LakeProperties();
            props.getBatch().getKubernetes().setApiUrl("http://127.0.0.1:" + server.getAddress().getPort());
            props.getBatch().getKubernetes().setPollSeconds(1);
            KubernetesJobExecutor executor = new KubernetesJobExecutor(props, new RecordingLogSink());

            BatchRun run = new BatchRun("run-k8s1", "init", "--job init", BatchRun.QUEUED, null,
                    Instant.now(), null, null, "/tmp/run-k8s1.log", "已排队");
            CompletableFuture<String> result = new CompletableFuture<>();
            String handle = executor.start(run, (r, status, code, elapsed, message) ->
                    result.complete(status + ":" + message));

            assertEquals("lake-run-k8s1", handle);
            assertEquals("SUCCEEDED:K8s Job 成功", result.get(10, TimeUnit.SECONDS));
            assertTrue(created.get().contains("run-k8s1"));
            assertTrue(created.get().contains("BATCH_RUN_ID"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void Pod日志会搬运到LogSink() throws Exception {
        AtomicReference<String> created = new AtomicReference<>();
        CountDownLatch logsServed = new CountDownLatch(1);
        String pod = "lake-run-k8s2-abcde";
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        // 状态接口要等日志接口先写完，否则作业可能在日志线程读之前就结束
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        String prefix = "/apis/batch/v1/namespaces/default/jobs";
        server.createContext(prefix, exchange -> {
            created.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] body = "{\"kind\":\"Job\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext(prefix + "/lake-run-k8s2", exchange -> {
            try {
                logsServed.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            byte[] body = "{\"status\":{\"succeeded\":1}}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext("/api/v1/namespaces/default/pods", exchange -> {
            byte[] body = ("{\"kind\":\"PodList\",\"items\":[{\"metadata\":{\"name\":\"" + pod + "\"}}]}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext("/api/v1/namespaces/default/pods/" + pod + "/log", exchange -> {
            byte[] body = "hello from pod\nsecond line\n".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
            logsServed.countDown();
        });
        server.start();
        try {
            LakeProperties props = new LakeProperties();
            props.getBatch().getKubernetes().setApiUrl("http://127.0.0.1:" + server.getAddress().getPort());
            props.getBatch().getKubernetes().setPollSeconds(1);
            props.getBatch().getKubernetes().setTimeoutSeconds(30);
            RecordingLogSink sink = new RecordingLogSink();
            KubernetesJobExecutor executor = new KubernetesJobExecutor(props, sink);

            BatchRun run = new BatchRun("run-k8s2", "show", "--job show", BatchRun.QUEUED, null,
                    Instant.now(), null, null, null, "已排队");
            CompletableFuture<String> result = new CompletableFuture<>();
            executor.start(run, (r, status, code, elapsed, message) ->
                    result.complete(status + ":" + message));

            assertEquals("SUCCEEDED:K8s Job 成功", result.get(10, TimeUnit.SECONDS));
            awaitLine(sink, "hello from pod");
            awaitLine(sink, "second line");
            assertTrue(created.get().contains("run-k8s2"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void jobJson会转义命令里的引号() {
        LakeProperties.Batch.Kubernetes conf = new LakeProperties().getBatch().getKubernetes();
        BatchRun run = new BatchRun("run-x", "init", "/bin/sh -c \"echo \\\"hi\\\"\"", BatchRun.QUEUED, null,
                Instant.now(), null, null, null, "已排队");
        String json = KubernetesJobExecutor.jobJson(run, "lake-run-x", conf);
        // 命令里的引号被转义，不会破坏 JSON 结构
        assertTrue(json.contains("\"backoffLimit\":0"));
        assertTrue(json.contains("BATCH_RUN_ID"));
        assertTrue(json.contains("\"lake-job\":\"lake-run-x\""), json);
        assertTrue(!json.contains("echo \"hi\""), json);
    }

    private static void awaitLine(RecordingLogSink sink, String expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            if (sink.lines.contains(expected)) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("没有收到日志行: " + expected + "，实际: " + sink.lines);
    }

    /** 只记录日志行，够验证搬运是否发生 */
    private static final class RecordingLogSink implements LogSink {
        private final List<String> lines = new CopyOnWriteArrayList<>();

        @Override
        public void append(String runId, List<String> batch) {
            lines.addAll(batch);
        }

        @Override
        public List<LogLine> tail(String runId, int lines) {
            return List.of();
        }

        @Override
        public List<LogLine> since(String runId, long afterSeq, int limit) {
            return List.of();
        }

        @Override
        public int purge(Instant before) {
            return 0;
        }
    }
}
