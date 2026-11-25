package com.demo.lake.server.governance.model;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/** 一张表的血缘：设计声明（上游/下游/列映射）+ 运行记录（哪次运行、哪个快照） */
public record LineageView(
        @Schema(description = "表名") String table,
        @Schema(description = "上游表，按顺序为直接上游再到更上游") List<String> upstream,
        @Schema(description = "下游表") List<String> downstream,
        @Schema(description = "产出该表的列级血缘") List<ColumnEdgeView> columns,
        @Schema(description = "ops.lineage_runs 里这张表的最近写入") List<RunView> runs) {

    public record ColumnEdgeView(String sourceTable, String sourceColumn,
                                 String targetTable, String targetColumn) {
    }

    public record RunView(String runId, String job, String snapshotId, Long recordedMs) {
    }
}
