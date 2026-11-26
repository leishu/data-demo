package com.demo.lake.server.security;

import com.demo.lake.server.config.LakeProperties;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 鉴权过滤器的纯函数部分：Basic 解析、口令匹配、路径 → 角色映射 */
class ApiAuthWebFilterTest {

    @Test
    void basic凭据解析支持冒号和大小写scheme() {
        String encoded = Base64.getEncoder().encodeToString("operator:p:ss".getBytes(StandardCharsets.UTF_8));
        ApiAuthWebFilter.Credentials creds = ApiAuthWebFilter.parseBasic("basic " + encoded);
        assertEquals("operator", creds.username());
        assertEquals("p:ss", creds.password());

        assertNull(ApiAuthWebFilter.parseBasic("Bearer abc"));
        assertNull(ApiAuthWebFilter.parseBasic("Basic !!!not-base64"));
    }

    @Test
    void noop口令前缀与定长比较() {
        assertTrue(ApiAuthWebFilter.passwordMatches("{noop}demo", "demo"));
        assertFalse(ApiAuthWebFilter.passwordMatches("{noop}demo", "Demo"));
        assertFalse(ApiAuthWebFilter.passwordMatches(null, "demo"));
    }

    @Test
    void 错误体JSON转义() {
        assertEquals("\"a\\\"b\"", ApiAuthWebFilter.quote("a\"b"));
        assertEquals("null", ApiAuthWebFilter.quote(null));
    }

    @Test
    void 接口按最小权限分角色() {
        assertEquals(List.of("QUERY"), ApiAuthWebFilter.requiredRoles("/api/query/stream", HttpMethod.POST));
        assertEquals(List.of("QUERY"), ApiAuthWebFilter.requiredRoles("/api/config", HttpMethod.GET));
        assertEquals(List.of("QUERY"), ApiAuthWebFilter.requiredRoles("/api/governance/assets", HttpMethod.GET));
        // 采样按角色切脱敏视图：auditor 读基础表明文，必须能进这个端点
        assertEquals(List.of("QUERY", "AUDITOR"),
                ApiAuthWebFilter.requiredRoles("/api/tables/silver_orders/sample", HttpMethod.GET));
        assertEquals(List.of("QUERY"), ApiAuthWebFilter.requiredRoles("/api/tables", HttpMethod.GET));
        assertEquals(List.of("QUERY"), ApiAuthWebFilter.requiredRoles("/api/batch/runs", HttpMethod.GET));
        assertEquals(List.of("BATCH"), ApiAuthWebFilter.requiredRoles("/api/batch/init", HttpMethod.POST));
        assertEquals(List.of("BATCH"), ApiAuthWebFilter.requiredRoles("/actuator/metrics", HttpMethod.GET));
        assertEquals(List.of("AUDITOR"), ApiAuthWebFilter.requiredRoles("/api/audit/sql/abc", HttpMethod.GET));
        // 事件列表同时放 QUERY / AUDITOR：auditor 只有 AUDITOR 角色，也要能看列表
        assertEquals(List.of("QUERY", "AUDITOR"), ApiAuthWebFilter.requiredRoles("/api/audit", HttpMethod.GET));
        assertTrue(ApiAuthWebFilter.requiredRoles("/api/unknown", HttpMethod.GET).isEmpty());
    }

    @Test
    void external模式解析网关身份头() {
        LakeProperties.Security conf = new LakeProperties().getSecurity();
        assertFalse(ApiAuthWebFilter.isExternal(conf));
        conf.setMode("EXTERNAL");
        assertTrue(ApiAuthWebFilter.isExternal(conf));
        conf.setMode("garbage");
        assertFalse(ApiAuthWebFilter.isExternal(conf));

        assertEquals("alice", ApiAuthWebFilter.firstNonBlank("", " alice ", "bob"));
        assertNull(ApiAuthWebFilter.firstNonBlank("", null));
        assertEquals(List.of("QUERY", "BATCH"),
                ApiAuthWebFilter.parseForwardedRoles(" QUERY, BATCH,QUERY, "));
        assertTrue(ApiAuthWebFilter.parseForwardedRoles(null).isEmpty());
    }

    @Test
    void 静态页面和探针公开其它接口私有() {
        assertTrue(ApiAuthWebFilter.isPublic("/"));
        assertTrue(ApiAuthWebFilter.isPublic("/ui/assets/index.js"));
        assertTrue(ApiAuthWebFilter.isPublic("/actuator/health"));
        assertTrue(ApiAuthWebFilter.isPublic("/actuator/health/liveness"));
        assertFalse(ApiAuthWebFilter.isPublic("/actuator/metrics"));
        assertFalse(ApiAuthWebFilter.isPublic("/api/config"));
    }
}
