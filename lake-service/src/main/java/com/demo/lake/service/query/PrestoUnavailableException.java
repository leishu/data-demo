package com.demo.lake.service.query;

/** Presto 连不上（没启动 / 网络不通）—— 依赖不可用，映射 503 */
public class PrestoUnavailableException extends RuntimeException {

    public PrestoUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
