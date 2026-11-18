package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.batch.model.RunsView;
import com.demo.lake.server.config.LakeProperties;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 批作业编排：按需拉起 Spark 作业的独立进程，把输出落到日志文件。
 *
 * <p>为什么不用同进程跑 Spark：Web 容器里常驻一个 SparkSession 会吃掉堆内存、和 Tomcat 抢线程，
 * 作业挂了还会拖死服务。子进程方式边界清晰，也贴合生产里「服务调 spark-submit」的做法——
 * 换集群时只改 lake.batch.command-template 即可。</p>
 *
 * <p>运行历史分两份：内存里的 {@link #runs} 是本进程的权威状态（含 RUNNING），
 * 湖表 {@code ops.batch_runs} 是重启后还能看到的终态归档（由 BatchApp 写入，
 * 见 {@link BatchHistoryService}）。列表接口把两边按 id 合并，内存优先。</p>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class BatchJobService {

    private static final int MAX_TAIL_LINES = 200;
    /** 开始时间倒序；历史记录时间解析失败时是 null，排到最后 */
    private static final Comparator<BatchRun> NEWEST_FIRST =
            Comparator.comparing(BatchRun::startedAt, Comparator.nullsLast(Comparator.<Instant>reverseOrder()));
    /** 子进程从这两个环境变量拿到服务端的运行 ID 与日志路径，写进 ops.batch_runs */
    static final String ENV_RUN_ID = "BATCH_RUN_ID";
    static final String ENV_RUN_LOG = "BATCH_RUN_LOG";

    private final LakeProperties props;
    private final BatchHistoryService history;
    private final Map<String, BatchRun> runs = new ConcurrentHashMap<>();
    private final Map<String, Process> liveProcesses = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public BatchRun submit(String job) {
        LakeProperties.Batch conf = props.getBatch();
        if (!conf.isEnabled()) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "批作业编排已关闭（lake.batch.enabled=false）");
        }
        if (job == null || !conf.getAllowedJobs().contains(job)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "不支持的作业: " + job + "，可选: " + conf.getAllowedJobs());
        }

        String command = conf.getCommandTemplate().replace("{job}", job);
        String id = "run-" + Instant.now().toEpochMilli() + "-" + UUID.randomUUID().toString().substring(0, 6);
        Path logFile = Paths.get(conf.getLogDir()).toAbsolutePath().resolve(id + ".log");
        try {
            Files.createDirectories(logFile.getParent());
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "无法创建日志目录: " + logFile.getParent(), e);
        }

        BatchRun run = new BatchRun(id, job, command, BatchRun.RUNNING, null,
                Instant.now(), null, null, logFile.toString(), "已提交");
        runs.put(id, run);
        log.info("提交批作业 {} -> {}", job, command);
        executor.submit(() -> execute(run, command, logFile));
        return run;
    }

    private void execute(BatchRun run, String command, Path logFile) {
        long start = System.currentTimeMillis();
        Process process = null;
        try {
            ProcessBuilder pb = new ProcessBuilder(tokenize(command))
                    .directory(new File(props.getBatch().getWorkDir()).getAbsoluteFile())
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.to(logFile.toFile()));
            // 让作业进程用自己的 run id 记一笔账，重启后两边才能按同一个主键对上
            pb.environment().put(ENV_RUN_ID, run.id());
            pb.environment().put(ENV_RUN_LOG, logFile.toString());
            process = pb.start();
            liveProcesses.put(run.id(), process);

            boolean finished = process.waitFor(props.getBatch().getTimeoutSeconds(), TimeUnit.SECONDS);
            long elapsed = System.currentTimeMillis() - start;
            if (!finished) {
                process.destroyForcibly();
                update(run, BatchRun.TIMEOUT, null, elapsed,
                        "超过 " + props.getBatch().getTimeoutSeconds() + "s 未结束，已强制结束");
                return;
            }
            int exit = process.exitValue();
            if (exit == 0) {
                update(run, BatchRun.SUCCEEDED, exit, elapsed, "完成");
            } else {
                update(run, BatchRun.FAILED, exit, elapsed, "退出码 " + exit + "，看日志尾部");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            long elapsed = System.currentTimeMillis() - start;
            update(run, BatchRun.FAILED, null, elapsed, "被中断");
        } catch (IOException e) {
            long elapsed = System.currentTimeMillis() - start;
            log.error("批作业 {} 启动失败", run.job(), e);
            update(run, BatchRun.FAILED, null, elapsed, "启动失败: " + e.getMessage());
        } finally {
            if (process != null) {
                liveProcesses.remove(run.id());
            }
        }
    }

    private void update(BatchRun run, String status, Integer exitCode, long elapsedMs, String message) {
        BatchRun done = run.finished(status, exitCode, message, elapsedMs);
        runs.put(done.id(), done);
        log.info("批作业 {} {} -> {} ({}ms)", run.job(), run.id(), status, elapsedMs);
    }

    /**
     * 内存记录 ∪ 湖表历史，按 id 去重（内存优先），再按开始时间倒序截断。
     *
     * <p>历史那份读不到不影响本接口：返回 {@link RunsView.History} 交给前端降级提示。</p>
     */
    public RunsView list(int limit) {
        int n = Math.max(1, Math.min(limit, 100));
        Map<String, BatchRun> merged = new LinkedHashMap<>();
        runs.values().stream().sorted(NEWEST_FIRST)
                .forEach(run -> merged.put(run.id(), run));

        BatchHistoryService.Result persisted = history.recent(n);
        for (BatchRun run : persisted.runs()) {
            merged.putIfAbsent(run.id(), run);
        }

        List<BatchRun> ordered = merged.values().stream()
                .sorted(NEWEST_FIRST)
                .limit(n)
                .toList();
        return new RunsView(ordered, persisted.history());
    }

    /** 先看内存，再回落到湖表（服务重启后仍能点开旧记录） */
    public BatchRun get(String id) {
        BatchRun run = runs.get(id);
        if (run != null) {
            return run;
        }
        BatchRun persisted = history.find(id);
        if (persisted != null) {
            return persisted;
        }
        throw new ResponseStatusException(HttpStatus.NOT_FOUND, "没有这个运行记录: " + id);
    }

    /** 读日志尾部，够定位失败原因就行，别把整个 spark 日志甩给调用方 */
    public List<String> tail(String id, int lines) {
        BatchRun run = get(id);
        int n = Math.max(1, Math.min(lines, MAX_TAIL_LINES));
        Path file = logFile(run);
        if (file == null || !Files.isReadable(file)) {
            // 历史记录可能来自别的机器/已被清理，没日志就返回空，不报错
            return List.of();
        }
        try {
            List<String> all = Files.readAllLines(file, StandardCharsets.UTF_8);
            return all.subList(Math.max(0, all.size() - n), all.size());
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

    /** 空白分词，支持单/双引号包住带空格的参数；不解析管道与重定向（那是 shell 的事） */
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
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("命令为空: " + command);
        }
        return tokens;
    }

    @PreDestroy
    void shutdown() {
        liveProcesses.forEach((id, p) -> {
            log.warn("服务关闭，强制结束作业进程 {}", id);
            p.destroyForcibly();
        });
        executor.shutdownNow();
    }
}
