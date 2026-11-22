package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.config.LakeProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Kubernetes 执行器：把一次批作业提交成 batch/v1 Job，虚拟线程轮询终态，并把 Pod 日志搬进 {@link LogSink}。
 *
 * <p>直接用 JDK HttpClient 调 API，不引 fabric8 客户端：只需要 create/get/delete、列 Pod、读 Pod 日志，
 * 少一个重型依赖也更容易在离线环境构建。ServiceAccount token 从 in-cluster 固定路径读取；
 * 本地调试可把 api-url 指到 HTTP 代理。TLS 证书需要按集群 CA 配 JVM truststore。</p>
 *
 * <p>日志搬运和本地执行器走同一个 {@link LogSink}：tail / SSE 从状态库读，不依赖 Pod 是否还在，
 * 也不要求请求落到提交 Job 的那台实例。Pod 还没创建时先按 {@code lake-job} 标签轮询，
 * 创建后用 follow 流持续读；作业结束/超时/取消时主动关流，避免虚拟线程挂在长连接上。</p>
 */
@Component("kubernetes")
@Slf4j
@RequiredArgsConstructor
public class KubernetesJobExecutor implements JobExecutor {

    private static final Path TOKEN = Path.of("/var/run/secrets/kubernetes.io/serviceaccount/token");
    private static final Pattern SUCCEEDED = Pattern.compile("\"succeeded\"\\s*:\\s*[1-9]");
    private static final Pattern FAILED = Pattern.compile("\"failed\"\\s*:\\s*[1-9]");
    /** PodList 里第一层 metadata.name；K8s 序列化时 name 在 generateName 之前 */
    private static final Pattern POD_NAME = Pattern.compile("\"metadata\"\\s*:\\s*\\{\\s*\"name\"\\s*:\\s*\"([^\"]+)\"");
    private static final int LOG_BATCH_LINES = 100;
    private static final long LOG_FLUSH_MILLIS = 1000;
    /** 作业结束后给日志流留一点时间把尾部读完，再强制关连接 */
    private static final long LOG_DRAIN_MILLIS = 2000;

    private final LakeProperties props;
    private final LogSink logSink;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final Map<String, LogStream> logStreams = new ConcurrentHashMap<>();

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
        long deadline = System.currentTimeMillis() + Math.max(30, conf.getTimeoutSeconds()) * 1000L;
        LogStream stream = new LogStream();
        logStreams.put(name, stream);
        stream.thread = Thread.ofVirtual().name("k8s-logs-" + name)
                .start(() -> shipLogs(run, name, conf, stream, deadline));
        Thread.ofVirtual().name("k8s-job-" + name).start(() -> {
            try {
                poll(run, name, statusUrl, conf, listener, start, deadline);
            } finally {
                stream.stopAndJoin();
                logStreams.remove(name, stream);
            }
        });
        return name;
    }

    private void poll(BatchRun run, String name, String statusUrl, LakeProperties.Batch.Kubernetes conf,
                      JobOutcomeListener listener, long start, long deadline) {
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
                // 轮询线程没了就必须报终态，否则心跳会一直给这个 run 续命，变成永久 RUNNING
                listener.completed(run, BatchRun.FAILED, 1, System.currentTimeMillis() - start,
                        "K8s Job 状态轮询被中断");
                return;
            } catch (Exception e) {
                log.debug("轮询 K8s Job {} 状态失败: {}", name, e.getMessage());
            }
            try {
                Thread.sleep(Math.max(1, conf.getPollSeconds()) * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.completed(run, BatchRun.FAILED, 1, System.currentTimeMillis() - start,
                        "K8s Job 状态轮询被中断");
                return;
            }
        }
        cancel(name);
        listener.completed(run, BatchRun.TIMEOUT, null, System.currentTimeMillis() - start, "K8s Job 等待超时");
    }

    /**
     * 找到 Job 创建的 Pod 并持续读它的 stdout/stderr，按批写进 LogSink。
     *
     * <p>Pod 名不自己拼：Job 控制器会加后缀，且创建有延迟。先按 {@code lake-job} 标签列 Pod，
     * 找到后再用 follow=true 读日志；日志接口在容器还没启动时可能返回 4xx，等一秒重试。</p>
     */
    private void shipLogs(BatchRun run, String jobName, LakeProperties.Batch.Kubernetes conf,
                          LogStream stream, long deadline) {
        try {
            String pod = awaitPod(jobName, conf, stream, deadline);
            if (pod == null) {
                log.debug("K8s Job {} 没有找到 Pod，跳过日志搬运", jobName);
                return;
            }
            String url = trimSlash(conf.getApiUrl()) + "/api/v1/namespaces/" + conf.getNamespace()
                    + "/pods/" + pod + "/log?follow=true&timestamps=false";
            while (!stream.stop.get() && System.currentTimeMillis() < deadline) {
                HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                        .header("Authorization", "Bearer " + token())
                        .header("Accept", "text/plain")
                        .GET().timeout(Duration.ofSeconds(10)).build();
                HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
                if (response.statusCode() / 100 != 2) {
                    response.body().close();
                    log.debug("K8s Pod {} 日志暂不可读: HTTP {}", pod, response.statusCode());
                    Thread.sleep(1000);
                    continue;
                }
                try (InputStream body = response.body()) {
                    stream.body = body;
                    streamLines(run.id(), body, stream);
                } finally {
                    stream.body = null;
                }
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // 作业结束/取消时主动关流会抛 IOException，不算错误
            if (!stream.stop.get()) {
                log.debug("搬运 K8s Job {} 日志失败: {}", jobName, e.getMessage());
            }
        }
    }

    /** 轮询 PodList 直到出现本 Job 的 Pod；超时/取消返回 null */
    private String awaitPod(String jobName, LakeProperties.Batch.Kubernetes conf,
                            LogStream stream, long deadline) throws InterruptedException {
        String url = trimSlash(conf.getApiUrl()) + "/api/v1/namespaces/" + conf.getNamespace()
                + "/pods?labelSelector=" + URLEncoder.encode("lake-job=" + jobName, StandardCharsets.UTF_8);
        while (!stream.stop.get() && System.currentTimeMillis() < deadline) {
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                        .header("Authorization", "Bearer " + token())
                        .GET().timeout(Duration.ofSeconds(10)).build();
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() / 100 == 2) {
                    Matcher matcher = POD_NAME.matcher(response.body());
                    if (matcher.find()) {
                        return matcher.group(1);
                    }
                }
            } catch (IOException e) {
                log.debug("查询 K8s Job {} 的 Pod 失败: {}", jobName, e.getMessage());
            }
            Thread.sleep(1000);
        }
        return null;
    }

    private void streamLines(String runId, InputStream body, LogStream stream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8));
        List<String> batch = new ArrayList<>(LOG_BATCH_LINES);
        long lastFlush = System.currentTimeMillis();
        try {
            String line;
            while (!stream.stop.get() && (line = reader.readLine()) != null) {
                batch.add(line);
                long now = System.currentTimeMillis();
                if (batch.size() >= LOG_BATCH_LINES || now - lastFlush >= LOG_FLUSH_MILLIS) {
                    flush(runId, batch);
                    lastFlush = now;
                }
            }
        } finally {
            flush(runId, batch);
        }
    }

    private void flush(String runId, List<String> batch) {
        if (batch.isEmpty()) {
            return;
        }
        logSink.append(runId, List.copyOf(batch));
        batch.clear();
    }

    @Override
    public void cancel(String handle) {
        if (handle == null || handle.equals("failed")) {
            return;
        }
        LogStream stream = logStreams.remove(handle);
        if (stream != null) {
            stream.stopAndJoin();
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

    /** 一条 Job 的日志流句柄：结束/取消时先让尾部读完，再强制关连接 */
    private static final class LogStream {
        private final AtomicBoolean stop = new AtomicBoolean();
        private volatile InputStream body;
        private volatile Thread thread;

        void stopAndJoin() {
            // 先等日志线程自然读到 EOF（作业刚结束时可能还有几行在管道里），
            // 等不到再置停止位并关流，避免虚拟线程挂在 follow 长连接上。
            Thread current = thread;
            if (current != null) {
                try {
                    current.join(LOG_DRAIN_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            stop.set(true);
            InputStream in = body;
            body = null;
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ignored) {
                    // 关流失败没有补救动作
                }
            }
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
        // Pod 打上 lake-job 标签，日志搬运时用它精确定位本 Job 的 Pod（不依赖旧的 job-name 标签）
        String labels = "{\"lake-job\":" + q(name) + "}";
        return "{\"apiVersion\":\"batch/v1\",\"kind\":\"Job\",\"metadata\":{\"name\":" + q(name)
                + ",\"namespace\":" + q(conf.getNamespace()) + ",\"labels\":" + labels + "},"
                + "\"spec\":{\"backoffLimit\":0,\"template\":{\"metadata\":{\"labels\":" + labels + "},\"spec\":{"
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
