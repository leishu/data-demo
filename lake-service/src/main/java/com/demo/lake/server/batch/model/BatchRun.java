package com.demo.lake.server.batch.model;

import com.demo.lake.common.batch.BatchRunStatus;

import java.time.Instant;

/**
 * 一次批作业运行的快照。字段不可变，状态变化时整体替换，避免并发读写半更新对象。
 *
 * <p>同一类型既表示内存里的实时运行态（含 RUNNING），也表示从 {@code ops.batch_runs}
 * 读回来的历史终态：两边字段一一对应，列表里才能直接按 id 合并。</p>
 */
public record BatchRun(
        String id,
        String job,
        String command,
        String status,
        Integer exitCode,
        Instant startedAt,
        Instant finishedAt,
        Long durationMs,
        String logFile,
        String message) {

    public static final String RUNNING = BatchRunStatus.RUNNING;
    public static final String SUCCEEDED = BatchRunStatus.SUCCEEDED;
    public static final String FAILED = BatchRunStatus.FAILED;
    public static final String TIMEOUT = BatchRunStatus.TIMEOUT;

    public BatchRun finished(String finalStatus, Integer code, String msg, long elapsedMs) {
        return new BatchRun(id, job, command, finalStatus, code, startedAt, Instant.now(), elapsedMs, logFile, msg);
    }
}
