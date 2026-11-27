package com.demo.lake.server.controller;

import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.service.AuditService;
import com.demo.lake.server.security.ApiAuthWebFilter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

/**
 * 控制台登录入口：两个公开端点，自己解析凭据，不经过鉴权过滤器的 challenge。
 *
 * <p>服务本身仍然无会话——登录成功后前端把凭据放在后续每个请求的 Basic 头里；
 * “退出”只是前端清掉本地凭据。这样既保留 Basic 的无状态特性，又让浏览器
 * 不会因为一次 401 就弹出原生登录框、把凭据缓存到关不掉的地方。</p>
 */
@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
@Tag(name = "认证", description = "控制台登录态：校验演示账号，返回当前身份（无服务端会话）")
public class AuthController {

    private final LakeProperties properties;
    private final AuditService audit;

    /** mode 回显给前端，便于提示 external 模式不需要在本页登录 */
    public record SessionView(String mode, boolean enabled, boolean authenticated,
                              String user, List<String> roles) {
    }

    public record LoginView(String user, List<String> roles, String mode) {
    }

    public record LoginRequest(String username, String password) {
    }

    @Operation(summary = "当前登录态", description = "公开端点：带 Basic 头就返回对应身份；没带或口令失效返回 authenticated=false，不返回 401")
    @GetMapping("/session")
    public SessionView session(@RequestHeader Map<String, String> requestHeaders) {
        LakeProperties.Security conf = properties.getSecurity();
        String mode = mode(conf);
        if (!conf.isEnabled()) {
            return new SessionView(mode, false, true, "anonymous", allRoles());
        }

        // 用 HttpHeaders 包一层，大小写不敏感；测试里直接传 Map 即可
        HttpHeaders headers = new HttpHeaders();
        if (requestHeaders != null) {
            requestHeaders.forEach(headers::set);
        }
        if (ApiAuthWebFilter.isExternal(conf)) {
            String secret = conf.getSharedSecret();
            if (secret == null || secret.isBlank()
                    || !ApiAuthWebFilter.constantEquals(secret, headers.getFirst(ApiAuthWebFilter.HEADER_SHARED_SECRET))) {
                return guest(mode);
            }
            String user = ApiAuthWebFilter.firstNonBlank(
                    headers.getFirst(ApiAuthWebFilter.HEADER_FORWARDED_USER),
                    headers.getFirst(ApiAuthWebFilter.HEADER_OAUTH_USER));
            if (user == null) {
                return guest(mode);
            }
            return new SessionView(mode, true, true, user,
                    ApiAuthWebFilter.parseForwardedRoles(headers.getFirst(ApiAuthWebFilter.HEADER_FORWARDED_ROLES)));
        }

        ApiAuthWebFilter.Credentials creds = ApiAuthWebFilter.parseBasic(headers.getFirst(HttpHeaders.AUTHORIZATION));
        if (creds == null) {
            return guest(mode);
        }
        LakeProperties.Security.Account account = findAccount(creds);
        return account == null ? guest(mode)
                : new SessionView(mode, true, true, account.getUsername(), account.getRoles());
    }

    @Operation(summary = "登录", description = "校验演示账号；成功后前端把凭据放进后续请求的 Basic 头")
    @PostMapping("/login")
    public LoginView login(@RequestBody LoginRequest request) {
        LakeProperties.Security conf = properties.getSecurity();
        String mode = mode(conf);
        if (!conf.isEnabled()) {
            return new LoginView("anonymous", allRoles(), mode);
        }
        if (ApiAuthWebFilter.isExternal(conf)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "认证由网关接管，请经网关访问，本页无需登录");
        }

        String username = request == null ? null : request.username();
        String password = request == null ? null : request.password();
        LakeProperties.Security.Account account = findAccount(
                new ApiAuthWebFilter.Credentials(username == null ? "" : username, password == null ? "" : password));
        if (account == null) {
            audit.record("auth.failed", username == null ? "anonymous" : username,
                    Map.of("path", "/api/auth/login", "reason", "bad-credentials"));
            // 不带 WWW-Authenticate：这里失败不该弹浏览器原生登录框
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "用户名或密码错误");
        }
        return new LoginView(account.getUsername(), account.getRoles(), mode);
    }

    private static SessionView guest(String mode) {
        return new SessionView(mode, true, false, null, List.of());
    }

    private static String mode(LakeProperties.Security conf) {
        return conf.getMode() == null ? "local" : conf.getMode();
    }

    private static List<String> allRoles() {
        return List.of("QUERY", "BATCH", "AUDITOR");
    }

    private LakeProperties.Security.Account findAccount(ApiAuthWebFilter.Credentials creds) {
        List<LakeProperties.Security.Account> accounts = properties.getSecurity().getAccounts();
        if (accounts == null) {
            return null;
        }
        return accounts.stream()
                .filter(a -> ApiAuthWebFilter.constantEquals(a.getUsername(), creds.username()))
                .filter(a -> ApiAuthWebFilter.passwordMatches(a.getPassword(), creds.password()))
                .findFirst().orElse(null);
    }
}
