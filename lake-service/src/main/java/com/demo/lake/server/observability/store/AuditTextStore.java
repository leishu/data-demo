package com.demo.lake.server.observability.store;

import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.support.StateDbDdl;
import com.demo.lake.server.support.StateDbTables;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.Types;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * 查询原文保险箱：普通审计日志只有指纹，这里单独存加密后的 SQL 文本。
 *
 * <p>按合规开关决定是否开启；开启但没有合法 AES-256 密钥时启动直接失败（fail closed），
 * 绝不退化成明文。读取需要 AUDITOR 角色，且每次读取都会再写一条审计事件。</p>
 */
@Component
@Slf4j
public class AuditTextStore {

    private static final String TABLE = StateDbTables.QUERY_TEXT;
    private static final Pattern EVENT_ID = Pattern.compile("[A-Za-z0-9-]{1,64}");
    private static final int NONCE_BYTES = 12;
    private static final SecureRandom RANDOM = new SecureRandom();

    private static final String INSERT_TEXT = """
            INSERT INTO %s (event_id, actor, client_ip, at_ms, expires_at,
            legal_hold, key_id, redacted, nonce, payload) VALUES (?,?,?,?,?,?,?,?,?,?)
            """.formatted(TABLE);
    private static final String FIND_TEXT = """
            SELECT actor, client_ip, at_ms, expires_at, legal_hold,
            key_id, redacted, nonce, payload FROM %s WHERE event_id=?
            """.formatted(TABLE);
    private static final String SET_LEGAL_HOLD = "UPDATE " + TABLE + " SET legal_hold=? WHERE event_id=?";
    private static final String PURGE_TEXT = "DELETE FROM " + TABLE + " WHERE expires_at<? AND NOT legal_hold";

    private final LakeProperties props;
    private final JdbcTemplate jdbc;
    private final MeterRegistry registry;
    /** 按 key_id 存的密钥环：轮换后旧编号仍然能解出老原文 */
    private final Map<String, SecretKeySpec> keys = new HashMap<>();
    private Counter decryptErrors;

    public AuditTextStore(LakeProperties props, @Qualifier("stateJdbc") JdbcTemplate jdbc,
                          MeterRegistry registry) {
        this.props = props;
        this.jdbc = jdbc;
        this.registry = registry;
    }

    @PostConstruct
    public void init() {
        if (!props.getAudit().isSqlCaptureEnabled()) {
            log.info("查询原文保险箱未开启（lake.audit.sql-capture-enabled=false）");
            return;
        }
        Map<String, String> configured = props.getAudit().getKeys();
        if (configured == null || configured.isEmpty()) {
            String encoded = props.getAudit().getKey();
            if (encoded == null || encoded.isBlank()) {
                throw new IllegalStateException("开启了查询原文保险箱但没配 LAKE_AUDIT_KEY，拒绝以明文启动");
            }
            configured = Map.of(props.getAudit().getKeyId(), encoded);
        }
        for (Map.Entry<String, String> entry : configured.entrySet()) {
            keys.put(entry.getKey(), decodeKey(entry.getValue()));
        }
        // LAKE_AUDIT_KEY 永远是「当前」密钥，优先于同编号的历史配置
        String current = props.getAudit().getKey();
        if (current != null && !current.isBlank()) {
            keys.put(props.getAudit().getKeyId(), decodeKey(current));
        }
        if (!keys.containsKey(props.getAudit().getKeyId())) {
            throw new IllegalStateException("审计密钥环里没有当前 key_id=" + props.getAudit().getKeyId()
                    + "，请先补齐 LAKE_AUDIT_KEY 或 lake.audit.keys." + props.getAudit().getKeyId());
        }
        decryptErrors = registry.counter("lake.audit.vault.decrypt.error");
        jdbc.execute(StateDbDdl.schemaAudit());
        jdbc.execute(StateDbDdl.queryText());
        log.info("查询原文保险箱就绪: {} (retention={}d)", TABLE, props.getAudit().getRetentionDays());
    }

    /** 审计事件落库后调用，eventId 用来把原文和普通审计事件关联起来 */
    public void save(String eventId, String actor, String clientIp, String sql) {
        if (!enabled() || eventId == null || !EVENT_ID.matcher(eventId).matches()) {
            return;
        }
        try {
            boolean redacted = props.getAudit().isRedact();
            String text = redacted ? SqlRedactor.redact(sql) : sql;
            byte[] nonce = new byte[NONCE_BYTES];
            RANDOM.nextBytes(nonce);
            byte[] payload = seal((text == null ? "" : text).getBytes(StandardCharsets.UTF_8), eventId, nonce,
                    keys.get(props.getAudit().getKeyId()));
            long now = System.currentTimeMillis();
            long expires = now + Math.max(1, props.getAudit().getRetentionDays()) * 86_400_000L;
            jdbc.update(INSERT_TEXT,
                    new Object[]{eventId, actor == null ? "-" : actor, clientIp, now, expires,
                            false, props.getAudit().getKeyId(), redacted, nonce, payload},
                    new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.BIGINT,
                            Types.BOOLEAN, Types.VARCHAR, Types.BOOLEAN, Types.BINARY, Types.BINARY});
        } catch (Exception e) {
            // 保险箱是合规能力，但不能让一次审计写入把用户查询搞挂
            log.error("查询原文写入保险箱失败 event={}: {}", eventId, e.getMessage());
        }
    }

    public Optional<TextRecord> find(String eventId) {
        if (!enabled() || eventId == null || !EVENT_ID.matcher(eventId).matches()) {
            return Optional.empty();
        }
        try {
            return jdbc.query(FIND_TEXT,
                    rs -> {
                        if (!rs.next()) {
                            return Optional.empty();
                        }
                        String rowKeyId = rs.getString("key_id");
                        SecretKeySpec rowKey = keys.get(rowKeyId);
                        if (rowKey == null) {
                            throw new IllegalStateException("审计密钥环缺少 key_id=" + rowKeyId
                                    + "（记录 event=" + eventId + "），请把旧密钥加进 lake.audit.keys 后重试");
                        }
                        byte[] nonce = rs.getBytes("nonce");
                        byte[] payload = rs.getBytes("payload");
                        byte[] plain;
                        try {
                            plain = open(payload, eventId, nonce, rowKey);
                        } catch (Exception e) {
                            throw new IllegalStateException("查询原文解密失败 event=" + eventId
                                    + " key_id=" + rowKeyId + "，密钥可能配错或数据已损坏", e);
                        }
                        String sql = new String(plain, StandardCharsets.UTF_8);
                        return Optional.of(new TextRecord(eventId, rs.getString("actor"), rs.getString("client_ip"),
                                rs.getLong("at_ms"), rs.getLong("expires_at"), rs.getBoolean("legal_hold"),
                                rowKeyId, rs.getBoolean("redacted"), sql));
                    }, eventId);
        } catch (DataAccessException e) {
            log.warn("读取查询原文失败 event={}: {}", eventId, e.getMessage());
            return Optional.empty();
        } catch (IllegalStateException e) {
            // 密钥/解密问题要和「记录不存在」区分开：审计员需要看到明确错误，监控也要能看到
            log.error("读取查询原文失败 event={}: {}", eventId, e.getMessage());
            decryptErrors.increment();
            throw e;
        }
    }

    /** 法律保全：置位后不再按 TTL 清理 */
    public boolean setLegalHold(String eventId, boolean hold) {
        if (!enabled() || eventId == null || !EVENT_ID.matcher(eventId).matches()) {
            return false;
        }
        try {
            return jdbc.update(SET_LEGAL_HOLD,
                    new Object[]{hold, eventId}, new int[]{Types.BOOLEAN, Types.VARCHAR}) > 0;
        } catch (DataAccessException e) {
            log.warn("设置法律保全失败 event={}: {}", eventId, e.getMessage());
            return false;
        }
    }

    /** 由既有对账循环调用 */
    public int purge(Instant now) {
        if (!enabled()) {
            return 0;
        }
        try {
            return jdbc.update(PURGE_TEXT, now.toEpochMilli());
        } catch (DataAccessException e) {
            log.warn("清理过期查询原文失败: {}", e.getMessage());
            return 0;
        }
    }

    public boolean enabled() {
        return props.getAudit().isSqlCaptureEnabled();
    }

    private static SecretKeySpec decodeKey(String encoded) {
        byte[] raw;
        try {
            raw = Base64.getDecoder().decode(encoded == null ? "" : encoded.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("审计密钥不是合法 base64", e);
        }
        if (raw.length != 32) {
            throw new IllegalStateException("审计密钥必须是 base64 编码的 32 字节 AES-256 密钥");
        }
        return new SecretKeySpec(raw, "AES");
    }

    private static byte[] seal(byte[] text, String eventId, byte[] nonce, SecretKeySpec key) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, nonce));
        cipher.updateAAD(eventId.getBytes(StandardCharsets.UTF_8));
        return cipher.doFinal(text);
    }

    private static byte[] open(byte[] payload, String eventId, byte[] nonce, SecretKeySpec key) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(128, nonce));
        cipher.updateAAD(eventId.getBytes(StandardCharsets.UTF_8));
        return cipher.doFinal(payload);
    }

    /** 审计员看到的原文记录；redacted=true 表示字符串字面量已被掩码 */
    public record TextRecord(String eventId, String actor, String clientIp, long atMs, long expiresAt,
                             boolean legalHold, String keyId, boolean redacted, String sql) {
    }
}
