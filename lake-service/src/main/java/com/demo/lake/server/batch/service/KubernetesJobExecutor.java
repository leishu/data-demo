package com.demo.lake.server.batch.service;

import com.demo.lake.common.batch.BatchRunStatus;
import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.config.LakeProperties;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kubernetes 执行器：把一次批作业提交成 batch/v1 Job，用 K8s Watch API 推送终态，并把 Pod 日志搬进 {@link LogSink}。
 *
 * <p>直接用 JDK HttpClient 调 API，不引入 fabric8：只需要 create/get-watch/delete、列 Pod watch、读 Pod 日志，
 * 少一个重型依赖也更容易在离线环境构建。ServiceAccount token 从 in-cluster 固定路径读取；
 * 本地调试可把 api-url 指到 HTTP 代理。TLS 证书需要按集群 CA 配 JVM truststore。</p>
 *
 * <p>状态不再按固定间隔轮询：对 Job 和 Pod 各开一条 {@code watch=true} 长连接，K8s 把
 * status/phase 变化推成 NDJSON 事件，毫秒级就能发现终态，也省掉了「隔几秒打一次 API」的空轮询。
 * 服务端按 timeoutSeconds 关流后带 resourceVersion 续接，410 Gone 则丢掉版本重新建连。</p>
 *
 * <p>日志搬运和本地执行器走同一个 {@link LogSink}：tail / SSE 从状态库读，不依赖 Pod 是否还在，
 * 也不要求请求落到提交 Job 的那台实例。先 watch Pod 到容器启动再读日志；
 * 作业结束/超时/取消时先把流读到 EOF，再强制关连接，避免虚拟线程挂在 watch / follow 长连接上。</p>
 */
@Component("kubernetes")
@Slf4j
@RequiredArgsConstructor
public class KubernetesJobExecutor implements JobExecutor {

    private static final Path TOKEN = Path.of("/var/run/secrets/kubernetes.io/serviceaccount/token");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int LOG_BATCH_LINES = 100;
    private static final long LOG_FLUSH_MILLIS = 1000;
    /** 作业结束后给日志流留一点时间把尾部读完，再强制关连接 */
    private static final long LOG_DRAIN_MILLIS = 2000;
    /** watch 长连接的服务端超时（秒），到期后带 resourceVersion 续接；不是业务的作业超时 */
    private static final int WATCH_TIMEOUT_SECONDS = 300;
    /** 只在建连失败 / 日志接口竞态时退避；正常流结束靠服务端 timeoutSeconds 自然收口 */
    private static final long RETRY_MILLIS = 300;

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
            listener.completed(run, BatchRunStatus.FAILED, null, elapsed(start),
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
                watchJob(run, name, statusUrl, conf, listener, start, deadline, stream);
            } finally {
                stream.stopAndJoin();
                logStreams.remove(name, stream);
            }
        });
        return name;
    }

    /**
     * watch Job 资源直到出现终态，取代按固定间隔 GET 状态：
     * K8s 把 status 变化推成 NDJSON 事件流，通常毫秒级就能拿到 SUCCEEDED/FAILED。
     * 服务端按 timeoutSeconds 关流后带 resourceVersion 续接，410 Gone 则丢掉版本重新建连。
     */
    private void watchJob(BatchRun run, String name, String statusUrl, LakeProperties.Batch.Kubernetes conf,
                          JobOutcomeListener listener, long start, long deadline, LogStream stream) {
        String resourceVersion = "";
        boolean terminal = false;
        try {
            while (System.currentTimeMillis() < deadline && !stream.stop.get()) {
                String url = statusUrl + "?watch=true&allowWatchBookmarks=true&timeoutSeconds="
                        + WATCH_TIMEOUT_SECONDS
                        + (resourceVersion.isEmpty() ? "" : "&resourceVersion=" + enc(resourceVersion));
                HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                        .header("Authorization", "Bearer " + token())
                        .header("Accept", "application/json")
                        .GET().timeout(Duration.ofSeconds(10)).build();
                try {
                    HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
                    int code = response.statusCode();
                    if (code == 404) {
                        response.body().close();
                        terminal = true;
                        listener.completed(run, BatchRunStatus.FAILED, 1, elapsed(start), "K8s Job 不存在");
                        return;
                    }
                    if (code == 410) {
                        response.body().close(); // 不关会让 HTTP 连接/FD 悬着，410 在续接时并不罕见
                        resourceVersion = ""; // 版本过期，丢掉续接点重新 watch
                        continue;
                    }
                    if (code / 100 != 2) {
                        response.body().close();
                        log.debug("watch K8s Job {} 失败: HTTP {}", name, code);
                        Thread.sleep(RETRY_MILLIS);
                        continue;
                    }
                    try (InputStream in = response.body();
                         BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                        stream.jobWatch = in;
                        String line;
                        while (!stream.stop.get() && (line = reader.readLine()) != null) {
                            if (System.currentTimeMillis() >= deadline) {
                                break;
                            }
                            WatchEvent event = parseEvent(line);
                            if (event == null) {
                                continue;
                            }
                            if (!event.resourceVersion().isEmpty()) {
                                resourceVersion = event.resourceVersion();
                            }
                            if ("ERROR".equals(event.type())) {
                                log.debug("K8s Job {} watch 返回错误事件: {}", name, line);
                                resourceVersion = "";
                                break; // 丢掉续接点重连
                            }
                            if (event.succeeded()) {
                                terminal = true;
                                listener.completed(run, BatchRunStatus.SUCCEEDED, 0, elapsed(start), "K8s Job 成功");
                                return;
                            }
                            if (event.failed()) {
                                terminal = true;
                                listener.completed(run, BatchRunStatus.FAILED, 1, elapsed(start), "K8s Job 失败");
                                return;
                            }
                        }
                    } finally {
                        stream.jobWatch = null;
                    }
                } catch (IOException e) {
                    if (stream.stop.get()) {
                        return;
                    }
                    log.debug("watch K8s Job {} 中断: {}", name, e.getMessage());
                    Thread.sleep(RETRY_MILLIS);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            listener.completed(run, BatchRunStatus.FAILED, 1, elapsed(start), "K8s Job 状态监听被中断");
            return;
        } catch (Exception e) {
            log.debug("watch K8s Job {} 异常: {}", name, e.getMessage());
        }
        if (terminal || stream.stop.get()) {
            return; // 终态已回调（或回调本身抛错），或外部取消：不再重复报终态
        }
        cancel(name);
        listener.completed(run, BatchRunStatus.TIMEOUT, null, elapsed(start), "K8s Job 等待超时");
    }

    /**
     * 找到本 Job 的 Pod 且容器已离开 Pending。
     *
     * <p>K8s 的 watch 不回放已存在的对象，所以先 list 一次捞出现有 Pod，再用 list 的
     * resourceVersion 建 watch 接增量。否则 Pod 若在 watch 建连前就跑到 Running，
     * 要等它结束才有下一个事件，而那时 stopAndJoin 很可能已经把日志流关掉，整段日志都会丢。</p>
     */
    private String awaitPod(String jobName, LakeProperties.Batch.Kubernetes conf,
                            LogStream stream, long deadline) throws InterruptedException {
        String base = trimSlash(conf.getApiUrl()) + "/api/v1/namespaces/" + conf.getNamespace() + "/pods";
        String label = enc("lake-job=" + jobName);
        String resourceVersion = "";
        String pod = null;

        // 1) 先 list：捞已存在的 Pod，并拿到续接版本；失败就退化为原来的纯 watch
        PodList listed = listPods(base + "?labelSelector=" + label);
        if (listed != null) {
            resourceVersion = listed.resourceVersion();
            for (WatchEvent event : listed.pods()) {
                pod = trackPod(pod, event);
                if (isReadyPod(event)) {
                    return event.name();
                }
            }
        }

        // 2) 再 watch 增量
        String watchUrl = base + "?watch=true&allowWatchBookmarks=true&timeoutSeconds=" + WATCH_TIMEOUT_SECONDS
                + "&labelSelector=" + label;
        while (!stream.stop.get() && System.currentTimeMillis() < deadline) {
            String url = watchUrl + (resourceVersion.isEmpty() ? "" : "&resourceVersion=" + enc(resourceVersion));
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .header("Authorization", "Bearer " + token())
                    .header("Accept", "application/json")
                    .GET().timeout(Duration.ofSeconds(10)).build();
            try {
                HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
                int code = response.statusCode();
                if (code == 410) {
                    response.body().close();
                    resourceVersion = "";
                    continue;
                }
                if (code / 100 != 2) {
                    response.body().close();
                    Thread.sleep(RETRY_MILLIS);
                    continue;
                }
                try (InputStream in = response.body();
                     BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    stream.podWatch = in;
                    String line;
                    while (!stream.stop.get() && (line = reader.readLine()) != null) {
                        if (System.currentTimeMillis() >= deadline) {
                            break;
                        }
                        WatchEvent event = parseEvent(line);
                        if (event == null || event.name().isEmpty()) {
                            continue;
                        }
                        if (!event.resourceVersion().isEmpty()) {
                            resourceVersion = event.resourceVersion();
                        }
                        if ("ERROR".equals(event.type())) {
                            resourceVersion = "";
                            break;
                        }
                        pod = trackPod(pod, event);
                        // Pending 阶段容器还没起，日志接口会 4xx；等 phase 推进再返回
                        if (isReadyPod(event)) {
                            return pod;
                        }
                    }
                } finally {
                    stream.podWatch = null;
                }
            } catch (IOException e) {
                if (stream.stop.get()) {
                    break;
                }
                log.debug("watch K8s Job {} 的 Pod 中断: {}", jobName, e.getMessage());
                Thread.sleep(RETRY_MILLIS);
            }
        }
        return pod; // 超时/取消时若已看到 Pod 名也返回，让日志线程再试一次
    }

    /** list 失败返回 null，调用方退化为纯 watch；假集群/老版本 API 都可能不支持 list */
    private PodList listPods(String url) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .header("Authorization", "Bearer " + token())
                .header("Accept", "application/json")
                .GET().timeout(Duration.ofSeconds(10)).build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                return null;
            }
            JsonNode root = JSON.readTree(response.body());
            if (root == null || !root.isObject()) {
                return null;
            }
            String rv = root.path("metadata").path("resourceVersion").asString("");
            List<WatchEvent> pods = new ArrayList<>();
            JsonNode items = root.path("items");
            if (items.isArray()) {
                for (JsonNode item : items) {
                    String itemRv = item.path("metadata").path("resourceVersion").asString("");
                    pods.add(new WatchEvent("ADDED", itemRv.isEmpty() ? rv : itemRv,
                            item.path("metadata").path("name").asString(""),
                            item.path("status").path("phase").asString(""), false, false));
                }
            }
            return new PodList(rv, pods);
        } catch (Exception e) {
            log.debug("列 K8s Pod 失败: {}", e.getMessage());
            return null;
        }
    }

    /** DELETED 只清掉同名 Pod，其余事件更新当前 Pod 名（Pending 也记下来等 phase 推进） */
    private static String trackPod(String current, WatchEvent event) {
        if (event == null || event.name().isEmpty()) {
            return current;
        }
        if ("DELETED".equals(event.type())) {
            return event.name().equals(current) ? null : current;
        }
        return event.name();
    }

    /** 容器已启动：日志接口不会再 4xx；phase 缺失也先当作没就绪 */
    private static boolean isReadyPod(WatchEvent event) {
        return event != null && !event.name().isEmpty()
                && !"DELETED".equals(event.type())
                && !event.phase().isEmpty()
                && !"Pending".equalsIgnoreCase(event.phase());
    }

    private record PodList(String resourceVersion, List<WatchEvent> pods) {
    }

    /**
     * 找到 Job 创建的 Pod 并持续读它的 stdout/stderr，按批写进 LogSink。
     *
     * <p>Pod 名不自己拼：Job 控制器会加后缀，且创建有延迟。先 watch Pod 的 labelSelector，
     * 拿到名字且容器启动后再用 follow=true 读日志。日志接口偶发 4xx（容器刚起的竞态）才短退避重试。</p>
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
                    Thread.sleep(RETRY_MILLIS);
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
        } finally {
            stream.logsDone.countDown();
        }
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

    /** watch NDJSON 事件流里只关心这几列：事件类型、续接版本、Pod 名/相位、Job 终态 */
    private record WatchEvent(String type, String resourceVersion, String name, String phase,
                              boolean succeeded, boolean failed) {
    }

    private static WatchEvent parseEvent(String line) {
        if (line == null || line.isBlank()) {
            return null;
        }
        try {
            JsonNode node = JSON.readTree(line);
            String type = node.path("type").asString("");
            JsonNode obj = node.get("object");
            if (obj == null || obj.isNull()) {
                return new WatchEvent(type, "", "", "", false, false);
            }
            String resourceVersion = obj.path("metadata").path("resourceVersion").asString("");
            String name = obj.path("metadata").path("name").asString("");
            String phase = obj.path("status").path("phase").asString("");
            long succeeded = obj.path("status").path("succeeded").asLong(0);
            long failed = obj.path("status").path("failed").asLong(0);
            JsonNode conditions = obj.path("status").path("conditions");
            if (conditions.isArray()) {
                for (JsonNode c : conditions) {
                    if ("Failed".equals(c.path("type").asString())
                            && "True".equalsIgnoreCase(c.path("status").asString())) {
                        failed = Math.max(failed, 1);
                        break;
                    }
                }
            }
            return new WatchEvent(type, resourceVersion, name, phase, succeeded > 0, failed > 0);
        } catch (Exception e) {
            log.debug("解析 K8s watch 事件失败: {}", e.getMessage());
            return null;
        }
    }

    /** 一条 Job 的日志流句柄：终态/取消时先等日志读到 EOF，再强制关掉所有长连接 */
    private static final class LogStream {
        private final AtomicBoolean stop = new AtomicBoolean();
        /** 日志线程读到 EOF（或异常退出）时倒数，取代固定 join 时间 */
        private final CountDownLatch logsDone = new CountDownLatch(1);
        private volatile InputStream body;
        private volatile InputStream jobWatch;
        private volatile InputStream podWatch;
        private volatile Thread thread;

        void stopAndJoin() {
            Thread current = thread;
            if (current != null) {
                try {
                    logsDone.await(LOG_DRAIN_MILLIS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            stop.set(true);
            close(body);
            body = null;
            close(jobWatch);
            jobWatch = null;
            close(podWatch);
            podWatch = null;
        }

        private static void close(InputStream in) {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ignored) {
                    // 关流失败没有补救动作
                }
            }
        }
    }

    private static long elapsed(long start) {
        return System.currentTimeMillis() - start;
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
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
