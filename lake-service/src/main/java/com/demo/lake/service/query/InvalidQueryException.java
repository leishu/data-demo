package com.demo.lake.service.query;

/** SQL 不合法（空、多语句、非只读）—— 属于调用方问题，映射 400 */
public class InvalidQueryException extends RuntimeException {

    public InvalidQueryException(String message) {
        super(message);
    }
}
