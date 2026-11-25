package com.demo.lake.server.observability.service;

import com.demo.lake.server.observability.model.AuditEvent;
import com.demo.lake.server.observability.store.AuditBuffer;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 结构化审计：一条事件既进 logs/audit.log（长期留存），也进内存环形缓冲（页面可查最近事件）。
 *
 * <p>只记身份、动作、对象 ID 和结果，不记 SQL 原文 / 口令 / 凭据；SQL 用 SHA-256 指纹代替。
 * 日志行是 {@code key=value} 形式，便于 grep 和被采集系统解析。</p>
 */
@Component
@RequiredArgsConstructor
public class AuditService {

    private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");
    private final AuditBuffer buffer;

    /** @return 事件 ID，用于把原文保险箱记录关联回这条审计事件 */
    public String record(String event, String actor, Map<String, Object> fields) {
        Instant at = Instant.now();
        String eventId = java.util.UUID.randomUUID().toString();
        Map<String, String> clean = new LinkedHashMap<>();
        fields.forEach((key, value) -> clean.put(sanitize(key), sanitize(String.valueOf(value))));
        clean.put("event_id", eventId);

        StringBuilder line = new StringBuilder("at=").append(at)
                .append(" event=").append(sanitize(event))
                .append(" actor=").append(sanitize(actor));
        clean.forEach((key, value) -> line.append(' ').append(key).append('=').append(value));
        AUDIT.info(line.toString());

        buffer.add(new AuditEvent(at, sanitize(event), sanitize(actor), clean));
        return eventId;
    }

    /** SQL / 敏感文本只留短指纹，既能关联同一条查询，又不泄露内容 */
    public static String fingerprint(String value) {
        if (value == null) {
            return "-";
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                sb.append(String.format("%02x", digest[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            return "unavailable";
        }
    }

    /** 日志是单行格式，字段里的空白/等号要收敛，避免伪造出新的 key=value */
    private static String sanitize(String value) {
        if (value == null) {
            return "-";
        }
        return value.trim().replaceAll("\\s+", "_").replace('=', ':').replace('"', '\'');
    }
}
