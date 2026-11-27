package com.demo.lake.server.query.service;

import lombok.experimental.StandardException;

/** Trino 连不上（没启动 / 网络不通）—— 依赖不可用，映射 503 */
@StandardException
public class TrinoUnavailableException extends RuntimeException {
}
