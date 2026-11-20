package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.config.LakeProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.regex.Pattern;

/**
 * Kubernetes 执行器：把一次批作业提交成 batch/v1 Job，虚拟线程轮询终态。
 *
 * <p>直接用 JDK HttpClient 调 API，不引 fabric8 客户端：只需要 create/get/delete 三个调用，
 * 少一个重型依赖也更容易在离线环境构建。ServiceAccount token 从 in-cluster 固定路径读取；
 * 本地调试可把 api-url 指到 HTTP 代理。TLS 证书需要按集群 CA 配 JVM truststore。</p>
 */
@Component("kubernetes")
@Slf4j
@RequiredArgsConstructor
public class KubernetesJobExecutor implements JobExecutor {

    private static final Path TOKEN = Path.of("/var/run/secrets/kubernetes.io/serviceaccount/token");
    private static final Pattern SUCCEEDED = Pattern.compile("\"succeeded\"\\s*:\\s*[1-9]");
    private static final Pattern FAILED = Pattern.compile("\"failed\"\\s*:\\s*[1-9]");

    private final LakeProperties props;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Override
    public String start(BatchRun run, JobOutcomeListener listener) {
        LakeProperties.Batch.Kubernetes conf = props.getBatch().getKubernetes();
        String name = "lake-" + run.id().replaceAll("[^a-z0-9-]", "").toLowerCase();
        String base = trimSlash(conf.getApiUrl()) + "/apis/batch/v1/namespaces/" + conf.getNamespace() + "/jobs";

        long start = System.currentTimeMillis();
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(base))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + token())
                    .POST(HttpRequest.BodyPublishers.ofString(jobJson(run, name, conf), StandardCharsets.UTF_8))
                    .timeout(Duration.ofSeconds(15))
                    .build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                throw new IllegalStateException("K8s 拒绝创建 Job: " + response.statusCode() + " " + response.body());
            }
        } catch (Exception e) {
            log.error("提交 K8s Job 失败 {}", run.id(), e);
            listener.completed(run, BatchRun.FAILED, null, System.currentTimeMillis() - start,
                    "提交 K8s Job 失败: " + e.getMessage());
            return "failed";
        }

        String statusUrl = base + "/" + name;
        Thread.ofVirtual().name("k8s-job-" + name).start(() -> poll(run, name, statusUrl, conf, listener, start));
        return name;
    }

    private void poll(BatchRun run, String name, String statusUrl, LakeProperties.Batch.Kubernetes conf,
                      JobOutcomeListener listener, long start) {
        long deadline = System.currentTimeMillis() + Math.max(30, conf.getTimeoutSeconds()) * 1000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create(statusUrl))
                        .header("Authorization", "Bearer " + token())
                        .GET().timeout(Duration.ofSeconds(10)).build();
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                String body = response.body();
                if (response.statusCode() == 404) {
                    listener.completed(run, BatchRun.FAILED, 1, System.currentTimeMillis() - start, "K8s Job 不存在");
                    return;
                }
                if (SUCCEEDED.matcher(body).find()) {
                    listener.completed(run, BatchRun.SUCCEEDED, 0, System.currentTimeMillis() - start, "K8s Job 成功");
                    return;
                }
                if (FAILED.matcher(body).find()) {
                    listener.completed(run, BatchRun.FAILED, 1, System.currentTimeMillis() - start, "K8s Job 失败");
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.debug("轮询 K8s Job {} 状态失败: {}", name, e.getMessage());
            }
            try {
                Thread.sleep(Math.max(1, conf.getPollSeconds()) * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        cancel(name);
        listener.completed(run, BatchRun.TIMEOUT, null, System.currentTimeMillis() - start, "K8s Job 等待超时");
    }

    @Override
    public void cancel(String handle) {
        if (handle == null || handle.equals("failed")) {
            return;
        }
        LakeProperties.Batch.Kubernetes conf = props.getBatch().getKubernetes();
        String url = trimSlash(conf.getApiUrl()) + "/apis/batch/v1/namespaces/" + conf.getNamespace() + "/jobs/" + handle;
        try {
            http.send(HttpRequest.newBuilder(URI.create(url))
                    .header("Authorization", "Bearer " + token())
                    .DELETE().timeout(Duration.ofSeconds(10)).build(),
                    HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            log.warn("删除 K8s Job {} 失败: {}", handle, e.getMessage());
        }
    }

    private String token() {
        try {
            return Files.isReadable(TOKEN) ? Files.readString(TOKEN).trim() : "";
        } catch (Exception e) {
            return "";
        }
    }

    private static String trimSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    /** 只拼固定字段；字符串统一转义，避免 command 里的引号破坏 JSON */
    static String jobJson(BatchRun run, String name, LakeProperties.Batch.Kubernetes conf) {
        return "{\"apiVersion\":\"batch/v1\",\"kind\":\"Job\",\"metadata\":{\"name\":" + q(name)
                + ",\"namespace\":" + q(conf.getNamespace()) + "},"
                + "\"spec\":{\"backoffLimit\":0,\"template\":{\"spec\":{"
                + "\"restartPolicy\":\"Never\",\"serviceAccountName\":" + q(conf.getServiceAccount()) + ","
                + "\"containers\":[{\"name\":\"batch\",\"image\":" + q(conf.getImage())
                + ",\"command\":[" + q("/bin/sh") + "," + q("-c") + "," + q(run.command()) + "],"
                + "\"env\":[{\"name\":\"BATCH_RUN_ID\",\"value\":" + q(run.id()) + "},"
                + "{\"name\":\"BATCH_RUN_LOG\",\"value\":" + q(String.valueOf(run.logFile())) + "}]}]}}}}";
    }

    private static String q(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}
