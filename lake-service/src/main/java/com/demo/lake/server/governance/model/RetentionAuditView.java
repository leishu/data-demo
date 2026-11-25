package com.demo.lake.server.governance.model;

import io.swagger.v3.oas.annotations.media.Schema;

/** 保留作业的一条审计事件，来自 ops.retention_audit */
public record RetentionAuditView(
        @Schema(description = "运行 ID") String runId,
        @Schema(description = "retention.start / retention.done") String event,
        @Schema(description = "表名") String table,
        @Schema(description = "true 表示 dry-run") boolean dryRun,
        @Schema(description = "命中的分区数") int partitions,
        @Schema(description = "命中的行数") long rowCount,
        @Schema(description = "epoch 毫秒") Long auditedMs,
        @Schema(description = "分区清单摘要") String detail) {
}
