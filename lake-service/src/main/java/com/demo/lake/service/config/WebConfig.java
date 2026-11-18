package com.demo.lake.service.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * 把 /ui/ 指向 Vue 的入口文件。
 *
 * <p>Spring 的静态资源处理器不会把目录请求映射到 index.html（只有根路径有 welcome page），
 * 所以 /ui/ 会变成 404。这里 forward 而不是 redirect，URL 保持 /ui/ 不变，前端 hash 路由照常工作。</p>
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/ui/").setViewName("forward:/ui/index.html");
    }
}
