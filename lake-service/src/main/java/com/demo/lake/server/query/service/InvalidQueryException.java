package com.demo.lake.server.query.service;

import lombok.experimental.StandardException;

/** SQL 不合法（空、多语句、非只读）—— 属于调用方问题，映射 400 */
@StandardException
public class InvalidQueryException extends RuntimeException {
}
