package com.demo.lake.common.lineage;

import lombok.Getter;

/** 列级血缘：源表某列参与产出目标表某列（聚合/派生也算，但不保证一一对应） */
@Getter
public final class ColumnEdge {

    private final String sourceTable;
    private final String sourceColumn;
    private final String targetTable;
    private final String targetColumn;

    public ColumnEdge(String sourceTable, String sourceColumn, String targetTable, String targetColumn) {
        this.sourceTable = sourceTable;
        this.sourceColumn = sourceColumn;
        this.targetTable = targetTable;
        this.targetColumn = targetColumn;
    }
}
