package com.demo.lake.servier.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** 把异常翻译成有信息量的 JSON，而不是一张 500 堆栈（状态码语义见 {@link ErrorMapper}） */
@RestControllerAdvice
@Slf4j
public class ApiExceptionHandler {

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> onError(Exception e) {
        ErrorMapper.Mapped mapped = ErrorMapper.map(e);
        if (mapped.status().is5xxServerError()) {
            log.error("未处理异常", e);
        } else {
            // 4xx 不打堆栈：SQL 写错、记录不存在都会走到这里，刷屏没有价值
            log.debug("请求失败({}): {}", mapped.status().value(), mapped.message());
        }
        return ResponseEntity.status(mapped.status())
                // 必须显式指定 JSON：SSE 接口上抛出来的异常也会走到这里，
                // 不写的话响应会被协商成 text/event-stream 然后 406
                .contentType(MediaType.APPLICATION_JSON)
                .body(mapped.body());
    }
}
