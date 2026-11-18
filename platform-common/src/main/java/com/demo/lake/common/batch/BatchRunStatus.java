package com.demo.lake.common.batch;

/**
 * 批作业运行状态。
 *
 * <p>放 platform-common 是因为两个模块都要用：spark-batch 侧往 {@code ops.batch_runs}
 * 只写终态（SUCCEEDED / FAILED），lake-service 侧除了终态还会产生 RUNNING / TIMEOUT
 * （超时是服务端强杀子进程判定的，作业进程自己不知道）。</p>
 */
public final class BatchRunStatus {

    public static final String RUNNING = "RUNNING";
    public static final String SUCCEEDED = "SUCCEEDED";
    public static final String FAILED = "FAILED";
    public static final String TIMEOUT = "TIMEOUT";

    private BatchRunStatus() {
    }

    /** 是否可以落表：只有终态值得持久化，RUNNING 留给内存 */
    public static boolean isTerminal(String status) {
        return SUCCEEDED.equals(status) || FAILED.equals(status) || TIMEOUT.equals(status);
    }
}
