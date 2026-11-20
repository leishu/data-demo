package com.demo.lake.server.query;

/** 语义层请求非法（指标/维度/过滤不在白名单里），映射成 400 */
public class InvalidSemanticQueryException extends RuntimeException {

    public InvalidSemanticQueryException(String message) {
        super(message);
    }
}
