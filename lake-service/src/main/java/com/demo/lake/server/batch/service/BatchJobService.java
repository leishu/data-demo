package com.demo.lake.server.batch.service;

import com.demo.lake.common.batch.BatchRunStatus;
import com.demo.lake.common.batch.RunIds;
import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.batch.model.RunsView;
import com.demo.lake.server.batch.store.LogStore;
import com.demo.lake.server.batch.store.RunStateStore;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.service.AuditService;
import com.demo.lake.server.observability.store.AuditTextStore;
import com.demo.lake.server.observability.service.LakeMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 批作业编排的入口：校验 → 幂等去重 → 落 QUEUED，剩下的交给 {@link JobDispatcher}。
 *
 * <p>这里不再 fork 子进程、也不在内存里维护运行态：状态在 Postgres（{@link RunStateStore}），
 * 执行方式由调度器选 local / Kubernetes。本类只负责对外的一致视图（列表、详情、日志尾部）
 * 和失联记录的心跳/对账。</p>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class BatchJobService {

    private static final int MAX_TAIL_LINES = 200;
    private static final int MAX_IDEMPOTENCY_KEY = 128;
    private static final Pattern IDEMPOTENCY_KEY = Pattern.compile("[A-Za-z0-9._:-]{1,128}");
    /** 开始时间倒序；历史记录时间解析失败时是 null，排到最后 */
    private static final Comparator<BatchRun> NEWEST_FIRST =
            Comparator.comparing(BatchRun::startedAt, Comparator.nullsLast(Comparator.<Instant>reverseOrder()));
    /** 子进程从这两个环境变量拿到服务端的运行 ID 与日志路径，写进 ops.batch_runs */
    static final String ENV_RUN_ID = "BATCH_RUN_ID";
    static final String ENV_RUN_LOG = "BATCH_RUN_LOG";

    private final LakeProperties props;
    private final BatchHistoryService history;
    private final RunStateStore state;
    private final LogStore logStore;
    private final BatchIdentity identity;
    private final RunningRunRegistry liveRuns;
    private final AuditService audit;
    private final AuditTextStore auditText;
    private final LakeMetrics metrics;
    private final Scheduler blocking;
    private final Disposable.Composite maintenance = Disposables.composite();

    @PostConstruct
    void startMaintenance() {
        LakeProperties.Batch.State conf = props.getBatch().getState();
        maintenance.add(Flux.interval(Duration.ofSeconds(Math.max(1, conf.getHeartbeatSeconds())))
                .flatMap(tick -> Mono.fromCallable(() -> {
                    // 只给「执行器还活着」的 run 续心跳；写终态失败时心跳也会停，交给对账收尸
                    for (String runId : liveRuns.snapshot()) {
                        try {
                            state.heartbeat(runId, Instant.now());
                        } catch (RuntimeException e) {
                            log.warn("run {} 心跳失败: {}", runId, e.getMessage());
                        }
                    }
                    return true;
                }).onErrorResume(e -> {
                    log.warn("运行状态心跳失败: {}", e.getMessage());
                    return Mono.empty();
                }).subscribeOn(blocking))
                .subscribe());

        maintenance.add(Flux.interval(Duration.ofSeconds(Math.max(5, conf.getReconcileSeconds())))
                .flatMap(tick -> Mono.fromCallable(() -> {
                    reconcile();
                    return true;
                }).onErrorResume(e -> {
                    log.warn("运行状态对账失败: {}", e.getMessage());
                    return Mono.empty();
                }).subscribeOn(blocking))
                .subscribe());
    }

    /** 失联的 RUNNING：能从湖表拿到终态就采纳，否则落「实例失联」终态；顺带清理过期日志行 */
    private void reconcile() {
        auditText.purge(Instant.now());
        int removed = logStore.purge(Instant.now().minus(
                Math.max(1, props.getBatch().getState().getLogRetentionDays()), java.time.temporal.ChronoUnit.DAYS));
        if (removed > 0) {
            log.debug("清理过期作业日志 {} 行", removed);
        }
        Instant cutoff = Instant.now().minusSeconds(Math.max(30, props.getBatch().getState().getStaleAfterSeconds()));
        for (BatchRun stale : state.staleRunning(cutoff)) {
            BatchRun archived = history.find(stale.id());
            if (isArchivedTerminal(archived)) {
                state.finish(archived);
                log.info("对账：{} 采纳湖表终态 {}", stale.id(), archived.status());
            } else {
                state.markLost(stale.id(), "运行实例失联（心跳超时），请查看作业日志", Instant.now());
                log.warn("对账：{} 没有可用终态，标记实例失联", stale.id());
            }
        }
    }

    /**
     * 只有已知的终态才能采纳：{@code BatchRunStatus.from} 对历史脏值返回 null，
     * 直接拿去 {@code finish} 会把 null 写进状态库的 NOT NULL 列，让对账每轮都失败。
     */
    static boolean isArchivedTerminal(BatchRun archived) {
        if (archived == null) {
            return false;
        }
        BatchRunStatus status = archived.status();
        return status != null && status.isTerminal();
    }

    /**
     * 提交作业：校验白名单后先落 QUEUED。带 Idempotency-Key 的重复请求直接返回原记录，
     * 不会重复起作业。
     */
    public BatchRun submit(String job, String idempotencyKey) {
        LakeProperties.Batch conf = props.getBatch();
        if (!conf.isEnabled()) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "批作业编排已关闭（lake.batch.enabled=false）");
        }
        if (job == null || !conf.getAllowedJobs().contains(job)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "不支持的作业: " + job + "，可选: " + conf.getAllowedJobs());
        }
        String key = idempotencyKey == null || idempotencyKey.isBlank() ? null : idempotencyKey.trim();
        if (key != null && (key.length() > MAX_IDEMPOTENCY_KEY || !IDEMPOTENCY_KEY.matcher(key).matches())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Idempotency-Key 只能是字母数字与 . _ : -，最长 128 字符");
        }
        if (key != null) {
            BatchRun existing = state.findByIdempotencyKey(key).orElse(null);
            if (existing != null) {
                log.info("幂等命中 {} -> {}", key, existing.id());
                return existing;
            }
        }

        String command = conf.getCommandTemplate().replace("{job}", job);
        String id = RunIds.generate();
        Path logFile = Paths.get(conf.getLogDir()).toAbsolutePath().resolve(id + ".log");
        try {
            Files.createDirectories(logFile.getParent());
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "无法创建日志目录: " + logFile.getParent(), e);
        }

        BatchRun run = new BatchRun(id, job, command, BatchRunStatus.QUEUED, null,
                Instant.now(), null, null, logFile.toString(), "已排队，等待调度");
        try {
            if (key == null) {
                state.insert(run, identity.id());
            } else {
                // 抢占幂等键 + 插入 run 在同一事务里；并发重复提交只有一个能赢
                String winner = state.insertWithIdempotency(run, identity.id(), key);
                if (!run.id().equals(winner)) {
                    log.info("幂等命中 {} -> {}", key, winner);
                    BatchRun existing = state.find(winner).orElse(null);
                    if (existing == null) {
                        throw new IllegalStateException("幂等键指向的运行记录不存在: " + winner);
                    }
                    return existing;
                }
            }
        } catch (RuntimeException e) {
            log.error("运行态写入失败，拒绝提交作业 {}", job, e);
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "运行状态库不可用，无法提交作业", e);
        }
        metrics.batchSubmitted(job);
        audit.record("batch.submit", "batch-service", java.util.Map.of(
                "run_id", id, "job", job, "idempotent", String.valueOf(key != null)));
        log.info("批作业已排队 {} -> {}", job, id);
        return run;
    }

    /** 运行状态库 ∪ 湖表历史，按 id 去重（终态优先），再按开始时间倒序截断 */
    public RunsView list(int limit) {
        int n = Math.clamp(limit, 1, 100);
        Map<String, BatchRun> merged = new LinkedHashMap<>();
        try {
            state.recent(n).forEach(run -> merged.put(run.id(), run));
        } catch (RuntimeException e) {
            log.warn("读运行状态失败（只显示湖表历史）: {}", e.getMessage());
        }

        BatchHistoryService.Result persisted = history.recent(n);
        for (BatchRun run : persisted.runs()) {
            BatchRun current = merged.get(run.id());
            if (current == null || (isTerminal(run.status()) && !isTerminal(current.status()))) {
                merged.put(run.id(), run);
            }
        }

        List<BatchRun> ordered = merged.values().stream()
                .sorted(NEWEST_FIRST)
                .limit(n)
                .toList();
        return new RunsView(ordered, persisted.history());
    }

    /**
     * 先看运行状态库，命中就直接返回（SSE 每 500ms 调一次，不能每次都去打 Trino）；
     * 只有状态库里没有的记录（命令行直跑、历史归档）才回落湖表。
     */
    public BatchRun get(String id) {
        try {
            BatchRun live = state.find(id).orElse(null);
            if (live != null) {
                return live;
            }
        } catch (RuntimeException e) {
            log.warn("读运行状态失败 {}: {}", id, e.getMessage());
        }
        try {
            BatchRun persisted = history.find(id);
            if (persisted != null) {
                return persisted;
            }
        } catch (RuntimeException e) {
            log.warn("读湖表历史失败 {}: {}", id, e.getMessage());
        }
        throw new ResponseStatusException(HttpStatus.NOT_FOUND, "没有这个运行记录: " + id);
    }

    private static boolean isTerminal(BatchRunStatus status) {
        return status != null && status.isTerminal();
    }

    /** 读日志尾部，够定位失败原因就行，别把整个 spark 日志甩给调用方 */
    public List<String> tail(String id, int lines) {
        BatchRun run = get(id);
        int n = Math.clamp(lines, 1, MAX_TAIL_LINES);
        // 优先读共享日志（跨实例可用）；老记录/命令行直跑的作业没写库，再退回本地文件
        List<LogStore.LogLine> shared = logStore.tail(id, n);
        if (!shared.isEmpty()) {
            return shared.stream().map(LogStore.LogLine::text).toList();
        }
        Path file = logFile(run);
        if (file == null || !Files.isReadable(file)) {
            // 历史记录可能来自别的机器/已被清理，没日志就返回空，不报错
            return List.of();
        }
        try {
            // 只从文件尾部读一块（最多 256KB），别把整个 spark 日志读进内存
            return LogTailCursor.readTail(file, n);
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "读日志失败: " + e.getMessage(), e);
        }
    }

    /** 日志路径可能为空（命令行直接跑的作业，或历史记录来自另一台机器） */
    static Path logFile(BatchRun run) {
        String path = run.logFile();
        if (path == null || path.isBlank()) {
            return null;
        }
        try {
            return Paths.get(path);
        } catch (RuntimeException e) {
            log.warn("非法日志路径: {}", path);
            return null;
        }
    }

    /** 空白分词，支持单/双引号包住带空格的参数；local 执行器复用它 */
    static List<String> tokenize(String command) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        char quote = 0;
        for (int i = 0; i < command.length(); i++) {
            char c = command.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                } else {
                    current.append(c);
                }
            } else if (c == '\'' || c == '"') {
                quote = c;
            } else if (Character.isWhitespace(c)) {
                if (!current.isEmpty()) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }
        if (!current.isEmpty()) {
            tokens.add(current.toString());
        }
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("命令为空: " + command);
        }
        return tokens;
    }

    @PreDestroy
    void shutdown() {
        maintenance.dispose();
    }
}
