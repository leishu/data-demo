package com.demo.lake.server.security;

import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.AuditService;
import com.demo.lake.server.observability.LakeMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * API 鉴权：HTTP Basic + 静态账号，无会话（每次请求都带凭据）。
 *
 * <p>为什么不上 Spring Security：本项目为了离线可构建锁定了 Boot 4 + Spring Security 7.0.x，
 * 本地仓库里没有完全对齐的 starter；对 demo 来说一个无状态 Basic 过滤器足够，而且行为完全可控。
 * 生产环境把 {@code lake.security.mode} 设为 external：网关完成认证后注入身份头，
 * 服务只认带共享密钥的请求；没配密钥时一律拒绝（fail closed）。见 docs/SECURITY.md。</p>
 *
 * <p>角色是接口级的：QUERY 能查表和跑 SQL，审计事件列表 QUERY / AUDITOR 都能看，BATCH 才能触发作业和看 actuator 指标；
 * 静态页面与健康检查公开，否则 K8s 探针和浏览器入口都会 401。</p>
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@Slf4j
@RequiredArgsConstructor
public class ApiAuthWebFilter implements WebFilter {

    /** external 模式：只有带正确共享密钥的请求，身份头才会被信任 */
    public static final String HEADER_SHARED_SECRET = "X-Auth-Shared-Secret";
    public static final String HEADER_FORWARDED_USER = "X-Forwarded-User";
    public static final String HEADER_OAUTH_USER = "X-Auth-Request-User";
    public static final String HEADER_FORWARDED_ROLES = "X-Forwarded-Roles";

    private final LakeProperties props;
    private final AuditService audit;
    private final LakeMetrics metrics;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        LakeProperties.Security conf = props.getSecurity();
        if (!conf.isEnabled() || isPublic(exchange.getRequest().getPath().value())) {
            return chain.filter(exchange);
        }

        String path = exchange.getRequest().getPath().value();
        List<String> required = requiredRoles(path, exchange.getRequest().getMethod());
        String user;
        List<String> roles;

        if (isExternal(conf)) {
            var headers = exchange.getRequest().getHeaders();
            // 密钥没配或不对就一律拒绝：绝不能因为「两边都是 null」而放过去
            if (conf.getSharedSecret() == null || conf.getSharedSecret().isBlank()
                    || !constantEquals(conf.getSharedSecret(), headers.getFirst(HEADER_SHARED_SECRET))) {
                log.debug("网关共享密钥校验失败: path={}", path);
                audit.record("auth.denied", "anonymous",
                        java.util.Map.of("path", path, "reason", "bad-shared-secret"));
                return deny(exchange, HttpStatus.UNAUTHORIZED, "网关凭据无效",
                        "本服务由网关保护，请经网关访问", "bad-shared-secret");
            }
            user = firstNonBlank(headers.getFirst(HEADER_FORWARDED_USER), headers.getFirst(HEADER_OAUTH_USER));
            if (user == null) {
                return deny(exchange, HttpStatus.UNAUTHORIZED, "缺少身份头",
                        "网关需要注入 X-Forwarded-User", "missing-identity");
            }
            roles = parseForwardedRoles(headers.getFirst(HEADER_FORWARDED_ROLES));
        } else {
            Credentials credentials = parseBasic(exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
            if (credentials == null) {
                return deny(exchange, HttpStatus.UNAUTHORIZED, "需要登录",
                        "用 viewer / operator 账号做 HTTP Basic 认证", "missing-credentials");
            }
            LakeProperties.Security.Account account = conf.getAccounts().stream()
                    .filter(a -> constantEquals(a.getUsername(), credentials.username()))
                    .filter(a -> passwordMatches(a.getPassword(), credentials.password()))
                    .findFirst().orElse(null);
            if (account == null) {
                log.debug("认证失败: path={} user={}", path, credentials.username());
                audit.record("auth.failed", credentials.username(),
                        java.util.Map.of("path", path, "reason", "bad-credentials"));
                return deny(exchange, HttpStatus.UNAUTHORIZED, "用户名或密码错误", null, "bad-credentials");
            }
            user = account.getUsername();
            roles = account.getRoles();
        }

        if (!required.isEmpty() && required.stream().noneMatch(roles::contains)) {
            String need = String.join("/", required);
            log.debug("权限不足: path={} user={} need={}", path, user, need);
            audit.record("auth.denied", user,
                    java.util.Map.of("path", path, "required_role", need));
            return deny(exchange, HttpStatus.FORBIDDEN, "账号没有 " + String.join(" 或 ", required) + " 权限",
                    "请联系管理员分配角色", "insufficient-role");
        }

        ApiPrincipal principal = new ApiPrincipal(user, List.copyOf(roles));
        // 下游控制器通过 Principal 拿到用户名，查询配额按账号分桶
        return chain.filter(exchange.mutate().principal(Mono.just(principal)).build());
    }

    /**
     * 静态页面、健康检查、错误页、接口文档与登录入口公开；其余一律要认证。
     *
     * <p>/api/auth/session 与 /api/auth/login 自己解析 Basic 头或请求体：
     * 它们不能先被过滤器challenge，否则登录页一加载就会弹浏览器原生登录框。</p>
     */
    static boolean isPublic(String path) {
        return path.equals("/api/auth/session") || path.equals("/api/auth/login")
                || path.equals("/") || path.equals("/index.html") || path.equals("/favicon.ico")
                || path.equals("/error")
                || path.equals("/ui") || path.startsWith("/ui/")
                || path.equals("/actuator/health") || path.startsWith("/actuator/health/")
                || path.equals("/actuator/info")
                // Swagger UI 与 OpenAPI 描述：文档本身不暴露数据，先让人看得到；
                // 真正的 /api/** 仍要认证，Describe 里声明了 basicAuth，可点 Authorize 后在线调试
                || path.equals("/swagger-ui.html") || path.startsWith("/swagger-ui/")
                || path.startsWith("/webjars/")
                || path.equals("/v3/api-docs") || path.startsWith("/v3/api-docs/");
    }

    /**
     * 路径 → 允许的角色集合，命中任一即可；空集合表示认证通过即可。
     *
     * <p>审计事件列表同时接受 QUERY 与 AUDITOR：列表只有身份/动作/结果 + SQL 指纹，
     * 不含原文；若只放 QUERY，auditor 能读原文却看不到列表，只能靠别人贴 event_id。</p>
     */
    static List<String> requiredRoles(String path, HttpMethod method) {
        if (path.startsWith("/api/query") || path.startsWith("/api/tables")
                || path.startsWith("/api/semantics") || path.equals("/api/config")) {
            return List.of("QUERY");
        }
        if (path.startsWith("/api/batch")) {
            // 看列表/详情只读，触发作业才是特权操作
            return List.of(HttpMethod.GET.equals(method) ? "QUERY" : "BATCH");
        }
        // 原文保险箱是特权数据：普通 BATCH / QUERY 角色也不够，必须 AUDITOR
        if (path.startsWith("/api/audit/sql")) {
            return List.of("AUDITOR");
        }
        if (path.startsWith("/api/audit")) {
            return List.of("QUERY", "AUDITOR");
        }
        if (path.startsWith("/actuator/")) {
            return List.of("BATCH");
        }
        return List.of();
    }

    /** external 模式才信任网关头；mode 拼错时按 local 处理，避免误把身份头当凭据 */
    public static boolean isExternal(LakeProperties.Security conf) {
        return "external".equalsIgnoreCase(conf.getMode());
    }

    /** 允许多个身份头别名（oauth2-proxy 用 X-Auth-Request-User），取第一个非空的 */
    public static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
    }

    /** X-Forwarded-Roles：逗号分隔，去空白与重复；空值就是没有任何角色 */
    public static List<String> parseForwardedRoles(String value) {
        List<String> roles = new ArrayList<>();
        if (value == null || value.isBlank()) {
            return roles;
        }
        for (String part : value.split(",")) {
            String role = part.trim();
            if (!role.isEmpty() && !roles.contains(role)) {
                roles.add(role);
            }
        }
        return roles;
    }

    /** Basic 凭据解析；大小写不敏感 scheme，用户名里允许冒号（只按第一个冒号切） */
    public static Credentials parseBasic(String header) {
        if (header == null || !header.regionMatches(true, 0, "Basic ", 0, 6)) {
            return null;
        }
        String encoded = header.substring(6).trim();
        if (encoded.isEmpty()) {
            return null;
        }
        String decoded;
        try {
            decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return null;
        }
        int colon = decoded.indexOf(':');
        return colon < 0 ? null : new Credentials(decoded.substring(0, colon), decoded.substring(colon + 1));
    }

    /** {noop} 前缀表示明文；生产应接 IdP / bcrypt，这里只防「密码写反了」这种低级错误 */
    public static boolean passwordMatches(String configured, String given) {
        if (configured == null) {
            return false;
        }
        String expected = configured.startsWith("{noop}") ? configured.substring(6) : configured;
        return constantEquals(expected, given);
    }

    /** 定长比较，避免用 equals 泄露前缀信息（demo 里也顺手做对） */
    public static boolean constantEquals(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    private Mono<Void> deny(ServerWebExchange exchange, HttpStatus status, String message, String hint, String reason) {
        metrics.authDenied(reason);
        if (status == HttpStatus.UNAUTHORIZED && "missing-credentials".equals(reason)) {
            audit.record("auth.denied", "anonymous",
                    java.util.Map.of("path", exchange.getRequest().getPath().value(), "reason", reason));
        }
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(status);
        if (status == HttpStatus.UNAUTHORIZED && "missing-credentials".equals(reason)) {
            // 只在「根本没带凭据」时挑战：浏览器据此弹原生登录框并缓存凭据。
            // 已经带了 Basic 头但口令错/过期时再挑战只会反复弹框，控制台自己的登录页会接手。
            response.getHeaders().set(HttpHeaders.WWW_AUTHENTICATE,
                    "Basic realm=\"" + props.getSecurity().getRealm() + "\", charset=\"UTF-8\"");
        }
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        // 本项目没引 Jackson（Boot 4 的 WebFlux starter 不带 databind），错误体字段固定，手写 JSON 即可
        String json = "{\"status\":" + status.value()
                + ",\"error\":" + quote(status.getReasonPhrase())
                + ",\"message\":" + quote(message)
                + ",\"hint\":" + quote(hint) + "}";
        return response.writeWith(Mono.just(response.bufferFactory().wrap(json.getBytes(StandardCharsets.UTF_8))));
    }

    /** 错误文案要塞进 JSON，至少把引号/反斜杠/控制字符转义掉 */
    static String quote(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    public record Credentials(String username, String password) {
    }
}
