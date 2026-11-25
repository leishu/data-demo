package com.demo.lake.server.governance.model;

import io.swagger.v3.oas.annotations.media.Schema;

/** 一条质量检查结果，来自 ops.dq_results；passed 由 observed / threshold 现算，不落库 */
public record DqResultView(
        @Schema(description = "所属运行 ID，可跳到 /jobs?run=xxx") String runId,
        @Schema(description = "被检查表的最近快照 ID") String snapshotId,
        @Schema(description = "检查项，如 silver_unique") String check,
        @Schema(description = "ERROR / WARN") String severity,
        @Schema(description = "实测值，越大越差") Double observed,
        @Schema(description = "允许上限") Double threshold,
        @Schema(description = "失败证据：重复订单号 / 异常行示例") String detail,
        @Schema(description = "检查时刻 epoch 毫秒") Long checkedMs,
        @Schema(description = "observed <= threshold") boolean passed) {
}
