package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.config.LakeProperties;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.File;
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
            listener.completed(run, BatchRun.FAILED, null, System.currentTimeMillis() - start,
                    "启动失败: " + e.getMessage());
            return "failed";
        }

        processes.put(run.id(), process);
        Thread.ofVirtual().name("job-" + run.id()).start(() -> {
            try {
                int timeout = props.getBatch().getTimeoutSeconds();
                boolean finished = process.waitFor(timeout, TimeUnit.SECONDS);
                long elapsed = System.currentTimeMillis() - start;
                if (!finished) {
                    process.destroyForcibly();
                    listener.completed(run, BatchRun.TIMEOUT, null, elapsed,
                            "超过 " + timeout + "s 未结束，已强制结束");
                    return;
                }
                int exit = process.exitValue();
                listener.completed(run, exit == 0 ? BatchRun.SUCCEEDED : BatchRun.FAILED, exit, elapsed,
                        exit == 0 ? "完成" : "退出码 " + exit + "，看日志尾部");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.completed(run, BatchRun.FAILED, null, System.currentTimeMillis() - start, "被中断");
            } finally {
                processes.remove(run.id());
            }
        });
        return "pid:" + process.pid();
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
