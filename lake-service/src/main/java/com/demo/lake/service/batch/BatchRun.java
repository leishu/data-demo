package com.demo.lake.service.batch;

import java.time.Instant;

/**
 * 一次批作业运行的快照。字段不可变，状态变化时整体替换，避免并发读写半更新对象。
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

    public static final String RUNNING = "RUNNING";
    public static final String SUCCEEDED = "SUCCEEDED";
    public static final String FAILED = "FAILED";
    public static final String TIMEOUT = "TIMEOUT";

    public BatchRun finished(String finalStatus, Integer code, String msg, long elapsedMs) {
        return new BatchRun(id, job, command, finalStatus, code, startedAt, Instant.now(), elapsedMs, logFile, msg);
    }
}
