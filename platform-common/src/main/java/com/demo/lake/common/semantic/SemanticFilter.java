package com.demo.lake.common.semantic;

/** 语义查询的过滤条件：字段必须是指标允许的维度，值由渲染器做类型化/转义 */
public final class SemanticFilter {

    private final String field;
    private final String op;
    private final String value;

    public SemanticFilter(String field, String op, String value) {
        this.field = field;
        this.op = op;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public String getOp() {
        return op;
    }

    public String getValue() {
        return value;
    }
}
