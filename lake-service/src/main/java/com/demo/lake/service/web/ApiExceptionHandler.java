package com.demo.lake.service.web;

import com.demo.lake.service.query.InvalidQueryException;
import com.demo.lake.service.query.PrestoUnavailableException;
import com.demo.lake.service.query.QueryFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/** 把异常翻译成有信息量的 JSON，而不是一张 500 堆栈 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(InvalidQueryException.class)
    public ResponseEntity<ApiError> onInvalidQuery(InvalidQueryException e) {
        return build(HttpStatus.BAD_REQUEST, e.getMessage(), "SQL 需为单条只读语句（select/with/show/describe/explain）");
    }

    @ExceptionHandler(QueryFailedException.class)
    public ResponseEntity<ApiError> onQueryFailed(QueryFailedException e) {
        String hint = e.getSqlState() == null ? null : "sqlState=" + e.getSqlState() + ", errorCode=" + e.getErrorCode();
        return build(HttpStatus.BAD_REQUEST, e.getMessage(), hint);
    }

    @ExceptionHandler(PrestoUnavailableException.class)
    public ResponseEntity<ApiError> onPrestoDown(PrestoUnavailableException e) {
        log.warn("Presto 不可用: {}", e.getMessage());
        return build(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage(),
                "先起 Presto：bin/infra.sh up；或改用 bin/presto-query.sh 的本地模式校验 SQL");
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> onBadBody(HttpMessageNotReadableException e) {
        return build(HttpStatus.BAD_REQUEST, "请求体不是合法 JSON", "期望 {\"sql\":\"select ...\",\"maxRows\":100}");
    }

    /** ResponseStatusException 自带状态码（作业白名单、记录不存在等），别被兜底吞掉 */
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiError> onStatus(ResponseStatusException e) {
        return build(HttpStatus.valueOf(e.getStatusCode().value()),
                e.getReason() == null ? e.getStatusCode().toString() : e.getReason(), null);
    }

    /** 静态资源缺失是 404，不该被下面的兜底吞成 500（前端页面刷到不存在的 assets 就靠它） */
    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ApiError> onNoResource(NoResourceFoundException e) {
        return build(HttpStatus.NOT_FOUND, "资源不存在: " + e.getResourcePath(), null);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> onUnexpected(Exception e) {
        log.error("未处理异常", e);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, e.getClass().getSimpleName() + ": " + e.getMessage(),
                "看服务日志定位");
    }

    private static ResponseEntity<ApiError> build(HttpStatus status, String message, String hint) {
        return ResponseEntity.status(status)
                .body(new ApiError(status.value(), status.getReasonPhrase(), message, hint));
    }
}
