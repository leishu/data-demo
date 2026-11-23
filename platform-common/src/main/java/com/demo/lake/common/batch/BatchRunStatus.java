package com.demo.lake.common.batch;

/**
 * 批作业运行状态。
 *
 * <p>放 platform-common 是因为两个模块都要用：spark-batch 侧往 {@code ops.batch_runs}
 * 只写终态（SUCCEEDED / FAILED），lake-service 侧还会产生 QUEUED（等调度名额）、RUNNING / TIMEOUT
 * （超时是服务端强杀子进程判定的，作业进程自己不知道）。</p>
 *
 * <p>枚举名就是落库 / JSON / 指标标签里用的值（{@link #name()}），中文描述只用于展示与日志，
 * 不要拿 {@code description()} 当持久化值。</p>
 */
public enum BatchRunStatus {

    QUEUED("排队中"),
    RUNNING("运行中"),
    SUCCEEDED("成功"),
    FAILED("失败"),
    TIMEOUT("超时");

    private final String description;

    BatchRunStatus(String description) {
        this.description = description;
    }

    /** 中文描述，供页面/日志展示；持久化一律用 name() */
    public String description() {
        return description;
    }

    /** 只有终态值得落表，RUNNING / QUEUED 是内存里的运行态 */
    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == TIMEOUT;
    }

    /** 宽松解析：null / 未知值返回 null，兼容历史数据与外部写入的脏行 */
    public static BatchRunStatus from(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        for (BatchRunStatus status : values()) {
            if (status.name().equalsIgnoreCase(trimmed)) {
                return status;
            }
        }
        return null;
    }
}
