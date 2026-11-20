package com.demo.lake.server.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI 元信息：文档标题/版本，以及全局的 HTTP Basic 认证声明。
 *
 * <p>{@code /api/**} 由 {@code ApiAuthWebFilter} 做 Basic 认证（demo 静态账号），
 * 这里把同一个认证方式写进 spec：Swagger UI 右上角会出现 Authorize 按钮，
 * 填 viewer / operator / auditor 就能直接 Try it out，不必再用 curl 拼 Authorization 头。</p>
 *
 * <p>角色差异（QUERY / BATCH / AUDITOR）由过滤器在服务端裁决，spec 里只在接口描述中说明，
 * 不假装 OpenAPI 能表达这套静态角色模型。</p>
 */
@Configuration
public class OpenApiConfig {

    private static final String BASIC_AUTH = "basicAuth";

    @Bean
    public OpenAPI lakeOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Lakehouse Demo API")
                        .description("""
                                湖仓服务层：Trino 即席查询、语义层、批作业编排与审计。

                                认证：除静态页面、健康检查与本文档外，所有接口需要 HTTP Basic 认证。
                                demo 账号：`viewer/viewer`（QUERY）、`operator/operator`（QUERY + BATCH）、
                                `auditor/auditor`（AUDITOR）。生产环境由网关 / IdP 接管认证。

                                长连接接口（`/api/query/stream`、`/api/batch/runs/{id}/events`）返回 SSE，
                                用 fetch + ReadableStream 解帧，EventSource 只支持 GET 的场合不适用。
                                """)
                        .version("1.0.0"))
                .components(new Components().addSecuritySchemes(BASIC_AUTH,
                        new SecurityScheme()
                                .type(SecurityScheme.Type.HTTP)
                                .scheme("basic")
                                .description("demo 静态账号：viewer / operator / auditor，用户名即密码")))
                .addSecurityItem(new SecurityRequirement().addList(BASIC_AUTH));
    }
}
