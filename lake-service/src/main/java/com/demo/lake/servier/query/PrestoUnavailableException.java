package com.demo.lake.servier.query;

import lombok.experimental.StandardException;

/** Presto 连不上（没启动 / 网络不通）—— 依赖不可用，映射 503 */
@StandardException
public class PrestoUnavailableException extends RuntimeException {
}
