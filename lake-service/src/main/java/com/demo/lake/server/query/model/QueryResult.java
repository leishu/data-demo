package com.demo.lake.server.query.model;

import java.util.List;

/** 一次查询的完整结果：列定义 + 行 + 是否截断 + 耗时 */
public record QueryResult(
        List<Column> columns,
        List<List<String>> rows,
        int rowCount,
        boolean truncated,
        long elapsedMs,
        String stats) {
}
