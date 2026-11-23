package com.demo.lake.server.batch.service;

import com.demo.lake.common.batch.BatchRunStatus;
import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.config.LakeProperties;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 本地执行器：用子进程跑 command-template（开发/演示默认）。
 *
 * <p>和以前的区别是它不再由提交线程直接等：Dispatcher 认领后才 start，
 * start 立即返回，等待/超时/回调都在虚拟线程里做，事件循环和调度循环都不被阻塞。</p>
 */
@Component("local")
@Slf4j
@RequiredArgsConstructor
public class LocalProcessJobExecutor implements JobExecutor {

    private final LakeProperties props;
    private final LogSink logSink;
    private final Map<String, Process> processes = new ConcurrentHashMap<>();

    @Override
    public String start(BatchRun run, JobOutcomeListener listener) {
        long start = System.currentTimeMillis();
        Process process;
        try {
            ProcessBuilder pb = new ProcessBuilder(BatchJobService.tokenize(run.command()))
                    .directory(new File(props.getBatch().getWorkDir()).getAbsoluteFile())
                    .redirectErrorStream(true);
            if (run.logFile() != null && !run.logFile().isBlank()) {
                pb.redirectOutput(ProcessBuilder.Redirect.to(new File(run.logFile())));
            } else {
                pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
            }
            pb.environment().put(BatchJobService.ENV_RUN_ID, run.id());
            if (run.logFile() != null) {
                pb.environment().put(BatchJobService.ENV_RUN_LOG, run.logFile());
            }
            process = pb.start();
        } catch (Exception e) {
            log.error("批作业 {} 启动失败", run.id(), e);
            listener.completed(run, BatchRunStatus.FAILED, null, System.currentTimeMillis() - start,
                    "启动失败: " + e.getMessage());
            return "failed";
        }

        processes.put(run.id(), process);
        Thread.ofVirtual().name("job-" + run.id()).start(() -> {
            LogTailCursor cursor = new LogTailCursor();
            Path logFile = run.logFile() == null || run.logFile().isBlank()
                    ? null : Path.of(run.logFile());
            int timeout = props.getBatch().getTimeoutSeconds();
            try {
                for (;;) {
                    // 边等边搬运日志：500ms 一次，SSE 和 tail 从状态库读，不再依赖本机磁盘
                    boolean finished = process.waitFor(500, TimeUnit.MILLISECONDS);
                    flush(logFile, cursor, run.id());
                    long elapsed = System.currentTimeMillis() - start;
                    if (finished) {
                        int exit = process.exitValue();
                        listener.completed(run, exit == 0 ? BatchRunStatus.SUCCEEDED : BatchRunStatus.FAILED, exit, elapsed,
                                exit == 0 ? "完成" : "退出码 " + exit + "，看日志尾部");
                        break;
                    }
                    if (elapsed >= timeout * 1000L) {
                        process.destroyForcibly();
                        listener.completed(run, BatchRunStatus.TIMEOUT, null, elapsed,
                                "超过 " + timeout + "s 未结束，已强制结束");
                        break;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.completed(run, BatchRunStatus.FAILED, null, System.currentTimeMillis() - start, "被中断");
            } finally {
                // 进程退出后可能还有几行没捞到，最后补一次
                flush(logFile, cursor, run.id());
                processes.remove(run.id());
            }
        });
        return "pid:" + process.pid();
    }

    private void flush(Path logFile, LogTailCursor cursor, String runId) {
        if (logFile == null) {
            return;
        }
        try {
            List<String> lines = cursor.poll(logFile, 500);
            if (!lines.isEmpty()) {
                logSink.append(runId, lines);
            }
        } catch (IOException e) {
            log.debug("读取作业日志失败 run={}: {}", runId, e.getMessage());
        }
    }

    @Override
    public void cancel(String handle) {
        if (handle == null || !handle.startsWith("pid:")) {
            return;
        }
        try {
            long pid = Long.parseLong(handle.substring(4));
            processes.values().stream().filter(p -> p.pid() == pid).forEach(Process::destroyForcibly);
        } catch (NumberFormatException e) {
            log.warn("非法本地进程句柄: {}", handle);
        }
    }

    @PreDestroy
    void shutdown() {
        processes.values().forEach(p -> {
            log.warn("服务关闭，强制结束作业进程 pid={}", p.pid());
            p.destroyForcibly();
        });
    }
}
