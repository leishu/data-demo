package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.config.LakeProperties;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 用 JDK 自带的 HttpServer 假扮 K8s API，验证提交与终态轮询，不依赖真实集群 */
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
            KubernetesJobExecutor executor = new KubernetesJobExecutor(props);

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
    void jobJson会转义命令里的引号() {
        LakeProperties.Batch.Kubernetes conf = new LakeProperties().getBatch().getKubernetes();
        BatchRun run = new BatchRun("run-x", "init", "/bin/sh -c \"echo \\\"hi\\\"\"", BatchRun.QUEUED, null,
                Instant.now(), null, null, null, "已排队");
        String json = KubernetesJobExecutor.jobJson(run, "lake-run-x", conf);
        // 命令里的引号被转义，不会破坏 JSON 结构
        assertTrue(json.contains("\"backoffLimit\":0"));
        assertTrue(json.contains("BATCH_RUN_ID"));
        assertTrue(!json.contains("echo \"hi\""), json);
    }
}
