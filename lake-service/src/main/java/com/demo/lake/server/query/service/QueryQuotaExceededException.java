package com.demo.lake.server.query.service;

/** 查询配额耗尽：映射成 429，而不是伪装成 Trino 不可用（503） */
public class QueryQuotaExceededException extends RuntimeException {

    public QueryQuotaExceededException(String message) {
        super(message);
    }
}
