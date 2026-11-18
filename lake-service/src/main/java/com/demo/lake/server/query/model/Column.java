package com.demo.lake.server.query.model;

/** 一列的名字与 Presto 类型名（前端按类型做右对齐/格式化） */
public record Column(String name, String type) {
}
