package com.demo.lake.servier.exception;

import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.resource.NoResourceFoundException;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 缺静态资源必须是 404：兜底的 Exception handler 不能把它吞成 500（前端加载旧 assets 时会踩到）。
 *
 * <p>WebFlux 下这个异常换到了 {@code org.springframework.web.reactive.resource}，
 * 且构造参数是 (资源根 URI, 相对路径)。</p>
 */
class ApiExceptionHandlerTest {

    private final ApiExceptionHandler handler = new ApiExceptionHandler();

    @Test
    void 缺失资源返回404而不是500() {
        NoResourceFoundException e =
                new NoResourceFoundException(URI.create("file:/tmp/static/"), "ui/assets/not-exist.js");

        ResponseEntity<ApiError> res = handler.onError(e);

        assertEquals(404, res.getStatusCode().value());
        assertEquals(404, res.getBody().status());
        assertTrue(res.getBody().message().contains("ui/assets/not-exist.js"),
                "错误体里要带出路径，便于定位是哪个资源没构建：" + res.getBody().message());
    }
}
