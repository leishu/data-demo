package com.demo.lake.common.lineage;

import lombok.Getter;

/** 表级血缘：source 表经过 transform 产出 target 表 */
@Getter
public final class TableEdge {

    private final String source;
    private final String target;
    /** 声明所在的转换 SQL 名，如 bronze_to_silver */
    private final String transform;

    public TableEdge(String source, String target, String transform) {
        this.source = source;
        this.target = target;
        this.transform = transform;
    }
}
