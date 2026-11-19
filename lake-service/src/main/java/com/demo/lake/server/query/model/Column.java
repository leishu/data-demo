package com.demo.lake.server.query.model;

/**
 * 一列的名字与 Trino 类型名（前端按类型做右对齐/格式化）。
 *
 * <p>comment 只在能从元数据拿到时才有值：表浏览走 information_schema.columns 读 DDL 里的
 * COMMENT（中文名）；即席查询的 ResultSetMetaData 给不出注释，这里就是 null。</p>
 */
public record Column(String name, String type, String comment) {

    public Column(String name, String type) {
        this(name, type, null);
    }
}
