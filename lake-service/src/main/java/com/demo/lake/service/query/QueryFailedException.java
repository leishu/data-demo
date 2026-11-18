package com.demo.lake.service.query;

/** Presto 拒绝了这条 SQL（语法错、表不存在等）—— 属于调用方问题，映射 400 */
public class QueryFailedException extends RuntimeException {

    private final String sqlState;
    private final Integer errorCode;

    public QueryFailedException(String message, String sqlState, Integer errorCode, Throwable cause) {
        super(message, cause);
        this.sqlState = sqlState;
        this.errorCode = errorCode;
    }

    public String getSqlState() {
        return sqlState;
    }

    public Integer getErrorCode() {
        return errorCode;
    }
}
