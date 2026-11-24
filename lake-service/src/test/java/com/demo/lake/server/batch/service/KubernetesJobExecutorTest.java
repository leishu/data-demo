package com.demo.lake.server.batch.service;

import com.demo.lake.common.batch.BatchRunStatus;
import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.config.LakeProperties;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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

/** 用 JDK 自带的 HttpServer 假扮 K8s API，验证提交、watch 终态与 Pod 日志搬运，不依赖真实集群 */
class KubernetesJobExecutorTest {

    @Test
    void 创建Job并watch到成功后回调() throws Exception {
        AtomicReference<String> created = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        String prefix = "/apis/batch/v1/namespaces/default/jobs";
        server.createContext(prefix, exchange -> {
            created.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] body = "{\"kind\":\"Job\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext(prefix + "/lake-run-k8s1", exchange -> sendNdjson(exchange,
                "{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"lake-run-k8s1\",\"resourceVersion\":\"1\"},\"status\":{}}}",
                "{\"type\":\"MODIFIED\",\"object\":{\"metadata\":{\"name\":\"lake-run-k8s1\",\"resourceVersion\":\"2\"},\"status\":{\"succeeded\":1}}}"));
        server.createContext("/api/v1/namespaces/default/pods", exchange -> sendNdjson(exchange,
                "{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"lake-run-k8s1-abcde\",\"resourceVersion\":\"1\"},\"status\":{\"phase\":\"Running\"}}}"));
        server.createContext("/api/v1/namespaces/default/pods/lake-run-k8s1-abcde/log", exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        try {
            LakeProperties props = new LakeProperties();
            props.getBatch().getKubernetes().setApiUrl("http://127.0.0.1:" + server.getAddress().getPort());
            props.getBatch().getKubernetes().setTimeoutSeconds(30);
            KubernetesJobExecutor executor = new KubernetesJobExecutor(props, new RecordingLogSink());

            BatchRun run = new BatchRun("run-k8s1", "init", "--job init", BatchRunStatus.QUEUED, null,
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
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        String prefix = "/apis/batch/v1/namespaces/default/jobs";
        server.createContext(prefix, exchange -> {
            created.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] body = "{\"kind\":\"Job\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        // 状态 watch 要等日志接口先写完，否则作业可能在日志线程读之前就结束
        server.createContext(prefix + "/lake-run-k8s2", exchange -> {
            try {
                logsServed.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            sendNdjson(exchange,
                    "{\"type\":\"MODIFIED\",\"object\":{\"metadata\":{\"name\":\"lake-run-k8s2\",\"resourceVersion\":\"2\"},\"status\":{\"succeeded\":1}}}");
        });
        server.createContext("/api/v1/namespaces/default/pods", exchange -> sendNdjson(exchange,
                "{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"" + pod + "\",\"resourceVersion\":\"1\"},\"status\":{\"phase\":\"Pending\"}}}",
                "{\"type\":\"MODIFIED\",\"object\":{\"metadata\":{\"name\":\"" + pod + "\",\"resourceVersion\":\"2\"},\"status\":{\"phase\":\"Running\"}}}"));
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
            props.getBatch().getKubernetes().setTimeoutSeconds(30);
            RecordingLogSink sink = new RecordingLogSink();
            KubernetesJobExecutor executor = new KubernetesJobExecutor(props, sink);

            BatchRun run = new BatchRun("run-k8s2", "show", "--job show", BatchRunStatus.QUEUED, null,
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

    /** Pod 在 watch 建连前就已 Running：watch 不回放已有对象，必须靠先 list 才捞得到 */
    @Test
    void 已存在的Pod靠list捞出即使watch没有事件() throws Exception {
        String pod = "lake-run-k8s3-abcde";
        CountDownLatch logsServed = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        String prefix = "/apis/batch/v1/namespaces/default/jobs";
        server.createContext(prefix, exchange -> {
            byte[] body = "{\"kind\":\"Job\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext(prefix + "/lake-run-k8s3", exchange -> {
            try {
                logsServed.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            sendNdjson(exchange,
                    "{\"type\":\"MODIFIED\",\"object\":{\"metadata\":{\"name\":\"lake-run-k8s3\",\"resourceVersion\":\"2\"},\"status\":{\"succeeded\":1}}}");
        });
        // 只返回 PodList（没有 watch 事件）：老实现只 watch 会一直等不到 Pod
        server.createContext("/api/v1/namespaces/default/pods", exchange -> sendNdjson(exchange,
                "{\"metadata\":{\"resourceVersion\":\"7\"},\"items\":["
                        + "{\"metadata\":{\"name\":\"" + pod + "\",\"resourceVersion\":\"6\"},"
                        + "\"status\":{\"phase\":\"Running\"}}]}"));
        server.createContext("/api/v1/namespaces/default/pods/" + pod + "/log", exchange -> {
            byte[] body = "from list\n".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
            logsServed.countDown();
        });
        server.start();
        try {
            LakeProperties props = new LakeProperties();
            props.getBatch().getKubernetes().setApiUrl("http://127.0.0.1:" + server.getAddress().getPort());
            props.getBatch().getKubernetes().setTimeoutSeconds(10);
            RecordingLogSink sink = new RecordingLogSink();
            KubernetesJobExecutor executor = new KubernetesJobExecutor(props, sink);
            BatchRun run = new BatchRun("run-k8s3", "show", "--job show", BatchRunStatus.QUEUED, null,
                    Instant.now(), null, null, null, "已排队");
            CompletableFuture<String> result = new CompletableFuture<>();
            executor.start(run, (r, status, code, elapsed, message) -> result.complete(status.name()));

            assertEquals("SUCCEEDED", result.get(10, TimeUnit.SECONDS));
            awaitLine(sink, "from list");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void jobJson会转义命令里的引号() {
        LakeProperties.Batch.Kubernetes conf = new LakeProperties().getBatch().getKubernetes();
        BatchRun run = new BatchRun("run-x", "init", "/bin/sh -c \"echo \\\"hi\\\"\"", BatchRunStatus.QUEUED, null,
                Instant.now(), null, null, null, "已排队");
        String json = KubernetesJobExecutor.jobJson(run, "lake-run-x", conf);
        // 命令里的引号被转义，不会破坏 JSON 结构
        assertTrue(json.contains("\"backoffLimit\":0"));
        assertTrue(json.contains("BATCH_RUN_ID"));
        assertTrue(json.contains("\"lake-job\":\"lake-run-x\""), json);
        assertTrue(!json.contains("echo \"hi\""), json);
    }

    /** 假 K8s watch 接口：按行返回 NDJSON 事件，读到流结束即为本次 watch 结束 */
    private static void sendNdjson(HttpExchange exchange, String... lines) throws IOException {
        byte[] body = (String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
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
