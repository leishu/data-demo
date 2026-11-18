package com.demo.lake.service.web;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 缺静态资源必须是 404：兜底的 Exception handler 不能把它吞成 500（前端加载旧 assets 时会踩到） */
class ApiExceptionHandlerTest {

    private final ApiExceptionHandler handler = new ApiExceptionHandler();

    @Test
    void 缺失资源返回404而不是500() {
        NoResourceFoundException e =
                new NoResourceFoundException(HttpMethod.GET, "ui/assets/not-exist.js", "/ui/assets/not-exist.js");

        ResponseEntity<ApiError> res = handler.onNoResource(e);

        assertEquals(404, res.getStatusCode().value());
        assertEquals(404, res.getBody().status());
        assertTrue(res.getBody().message().contains("ui/assets/not-exist.js"));
    }
}
