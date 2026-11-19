package com.demo.lake.server.query.model;

import java.util.List;

/** 表浏览用的一张表：表名 + 类型 + 字段清单（字段带 COMMENT 里的中文注释） */
public record TableInfo(String schema, String table, String type, List<Column> columns) {
}
