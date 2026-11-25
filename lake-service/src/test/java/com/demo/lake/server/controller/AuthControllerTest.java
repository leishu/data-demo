package com.demo.lake.server.controller;

import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.store.AuditBuffer;
import com.demo.lake.server.observability.model.AuditEvent;
import com.demo.lake.server.observability.service.AuditService;
import com.demo.lake.server.security.ApiAuthWebFilter;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 控制台登录入口：登录校验与登录态回显（不触发浏览器原生 Basic 弹框） */
class AuthControllerTest {

    private final LakeProperties props = new LakeProperties();
    private final AuditBuffer buffer = new AuditBuffer();
    private final AuthController controller = new AuthController(props, new AuditService(buffer));

    @Test
    void 本地登录返回账号角色() {
        AuthController.LoginView view = controller.login(new AuthController.LoginRequest("operator", "operator"));
        assertEquals("operator", view.user());
        assertEquals(List.of("QUERY", "BATCH"), view.roles());
        assertEquals("local", view.mode());
    }

    @Test
    void 口令错误抛401并记审计() {
        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> controller.login(new AuthController.LoginRequest("operator", "wrong")));
        assertEquals(HttpStatus.UNAUTHORIZED, e.getStatusCode());

        List<AuditEvent> events = buffer.recent(10);
        assertEquals(1, events.size());
        assertEquals("auth.failed", events.get(0).event());
        assertEquals("operator", events.get(0).actor());
    }

    @Test
    void session没带凭据只回登录态不抛401() {
        AuthController.SessionView view = controller.session(Map.of());
        assertEquals("local", view.mode());
        assertTrue(view.enabled());
        assertFalse(view.authenticated());
        assertNull(view.user());
    }

    @Test
    void session识别正确与失效的Basic头() {
        Map<String, String> ok = Map.of(HttpHeaders.AUTHORIZATION, basic("auditor", "auditor"));
        AuthController.SessionView view = controller.session(ok);
        assertTrue(view.authenticated());
        assertEquals("auditor", view.user());
        assertEquals(List.of("AUDITOR"), view.roles());

        AuthController.SessionView bad = controller.session(Map.of(HttpHeaders.AUTHORIZATION, basic("auditor", "nope")));
        assertFalse(bad.authenticated());
        assertNull(bad.user());
    }

    @Test
    void 关闭认证时直接放行() {
        props.getSecurity().setEnabled(false);
        AuthController.SessionView session = controller.session(Map.of());
        assertFalse(session.enabled());
        assertTrue(session.authenticated());
        assertEquals("anonymous", session.user());

        AuthController.LoginView login = controller.login(null);
        assertEquals("anonymous", login.user());
        assertEquals(3, login.roles().size());
    }

    @Test
    void external模式校验共享密钥与身份头() {
        LakeProperties.Security conf = props.getSecurity();
        conf.setMode("external");
        conf.setSharedSecret("shared-secret");

        Map<String, String> headers = Map.of(
                ApiAuthWebFilter.HEADER_SHARED_SECRET, "shared-secret",
                ApiAuthWebFilter.HEADER_FORWARDED_USER, "alice",
                ApiAuthWebFilter.HEADER_FORWARDED_ROLES, "QUERY,BATCH");
        AuthController.SessionView view = controller.session(headers);
        assertTrue(view.authenticated());
        assertEquals("alice", view.user());
        assertEquals(List.of("QUERY", "BATCH"), view.roles());

        Map<String, String> wrongSecret = Map.of(
                ApiAuthWebFilter.HEADER_SHARED_SECRET, "wrong",
                ApiAuthWebFilter.HEADER_FORWARDED_USER, "alice");
        assertFalse(controller.session(wrongSecret).authenticated());

        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> controller.login(new AuthController.LoginRequest("operator", "operator")));
        assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
    }

    private static String basic(String user, String password) {
        String token = Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
        return "Basic " + token;
    }
}
