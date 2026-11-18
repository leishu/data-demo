package com.demo.lake.servier.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.net.URI;

/**
 * 把 /ui 与 /ui/ 指向 Vue 的入口文件。
 *
 * <p>WebFlux 的静态资源处理器同样不会把目录请求映射到 index.html（只有根路径有 welcome page），
 * 所以 /ui 会 404。WebFlux 没有 MVC 的 ViewControllerRegistry，这里用 router function 跳过去——
 * 前端是 hash 路由，跳一次不影响 #/query 这类深链。</p>
 */
@Configuration
public class WebConfig {

    @Bean
    public RouterFunction<ServerResponse> uiRoutes() {
        return RouterFunctions.route()
                .GET("/ui", request -> toIndex())
                .GET("/ui/", request -> toIndex())
                .build();
    }

    private static Mono<ServerResponse> toIndex() {
        return ServerResponse.temporaryRedirect(URI.create("/ui/index.html")).build();
    }
}
