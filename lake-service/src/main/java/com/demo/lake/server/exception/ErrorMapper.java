package com.demo.lake.server.exception;

import com.demo.lake.server.query.service.InvalidQueryException;
import com.demo.lake.server.query.service.InvalidSemanticQueryException;
import com.demo.lake.server.query.service.QueryFailedException;
import com.demo.lake.server.query.service.QueryQuotaExceededException;
import com.demo.lake.server.query.service.TrinoUnavailableException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebInputException;

/**
 * 异常 → (HTTP 状态, 文案, 提示) 的统一映射。
 *
 * <p>为什么单独抽出来：JSON 接口交给 {@link ApiExceptionHandler}，而 SSE 接口用不了它——
 * 响应头在第一个事件发出时就已经是 200，流里的错误只能自己翻成一条 error 事件。</p>
 */
public final class ErrorMapper {

    public record Mapped(HttpStatus status, String message, String hint) {

        public ApiError body() {
            return new ApiError(status.value(), status.getReasonPhrase(), message, hint);
        }
    }

    private ErrorMapper() {
    }

    public static Mapped map(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause() == current ? null : current.getCause()) {
            Mapped mapped = direct(current);
            if (mapped != null) {
                return mapped;
            }
        }
        String msg = error.getMessage() == null ? error.toString() : error.getMessage();
        return new Mapped(HttpStatus.INTERNAL_SERVER_ERROR,
                error.getClass().getSimpleName() + ": " + msg, "看服务日志定位");
    }

    private static Mapped direct(Throwable current) {
        if (current instanceof InvalidQueryException) {
            return new Mapped(HttpStatus.BAD_REQUEST, current.getMessage(),
                    "SQL 需为单条只读语句（select/with/show/describe/explain）");
        }
        if (current instanceof InvalidSemanticQueryException) {
            return new Mapped(HttpStatus.BAD_REQUEST, current.getMessage(),
                    "检查 metric / dimensions / filters 是否在指标目录的白名单内");
        }
        if (current instanceof QueryFailedException failed) {
            String hint = failed.getSqlState() == null
                    ? null : "sqlState=" + failed.getSqlState() + ", errorCode=" + failed.getErrorCode();
            return new Mapped(HttpStatus.BAD_REQUEST, failed.getMessage(), hint);
        }
        if (current instanceof TrinoUnavailableException) {
            return new Mapped(HttpStatus.SERVICE_UNAVAILABLE, current.getMessage(),
                    "先起 Trino：bin/infra.sh up trino；或改用 bin/trino-query.sh 的本地模式校验 SQL");
        }
        // 配额是「客户端发太多」，不是依赖挂了，必须 429 而不是 503
        if (current instanceof QueryQuotaExceededException) {
            return new Mapped(HttpStatus.TOO_MANY_REQUESTS, current.getMessage(),
                    "调大 lake.query 下的 max-concurrent-queries / max-queries-per-minute，或稍后重试");
        }
        // ServerWebInputException 是 ResponseStatusException 的子类，必须排在它前面
        if (current instanceof ServerWebInputException) {
            return new Mapped(HttpStatus.BAD_REQUEST, "请求体不是合法 JSON",
                    "期望 {\"sql\":\"select ...\",\"maxRows\":100}");
        }
        // 静态资源缺失 / 作业白名单 / 记录不存在都走这里；WebFlux 下 404 是 NoResourceFoundException(404)
        if (current instanceof ResponseStatusException rse) {
            HttpStatus status = HttpStatus.resolve(rse.getStatusCode().value());
            return new Mapped(status == null ? HttpStatus.INTERNAL_SERVER_ERROR : status,
                    rse.getReason() == null ? rse.getStatusCode().toString() : rse.getReason(), null);
        }
        return null;
    }
}
