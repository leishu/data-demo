package com.demo.lake.common.quality;

import lombok.Getter;
import lombok.experimental.Accessors;

/** 质量检查级别：ERROR 会让 quality 作业失败，WARN 只记录不阻断 */
@Getter
@Accessors(fluent = true)
public enum QualitySeverity {

    ERROR(true),
    WARN(false);

    private final boolean failsJob;

    QualitySeverity(boolean failsJob) {
        this.failsJob = failsJob;
    }
}
