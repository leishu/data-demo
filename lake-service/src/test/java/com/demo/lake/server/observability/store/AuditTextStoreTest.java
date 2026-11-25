package com.demo.lake.server.observability.store;

import com.demo.lake.server.config.LakeProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 保险箱的开关、fail-closed、脱敏加密、保全与清理 */
class AuditTextStoreTest {

    private static final String KEY =
            Base64.getEncoder().encodeToString("0123456789abcdef0123456789abcdef".getBytes());

    private AuditTextStore store(boolean enabled, String key) {
        LakeProperties props = new LakeProperties();
        props.getAudit().setSqlCaptureEnabled(enabled);
        props.getAudit().setKey(key);
        props.getAudit().setRetentionDays(1);
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:vault-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        AuditTextStore store = new AuditTextStore(props, new JdbcTemplate(ds), new SimpleMeterRegistry());
        store.init();
        return store;
    }

    @Test
    void 写入时脱敏加密读取时解密() {
        AuditTextStore store = store(true, KEY);
        store.save("evt-1", "operator", "127.0.0.1",
                "select * from t where password = 'hunter2' and dt = '2026-11-20'");

        AuditTextStore.TextRecord record = store.find("evt-1").orElseThrow();
        assertTrue(record.redacted());
        assertEquals("operator", record.actor());
        assertEquals("127.0.0.1", record.clientIp());
        assertTrue(record.sql().contains("dt = '2026-11-20'"), record.sql());
        assertFalse(record.sql().contains("hunter2"), record.sql());
    }

    @Test
    void 关闭时既不写也不读() {
        AuditTextStore store = store(false, null);
        store.save("evt-2", "operator", null, "select 1");
        assertTrue(store.find("evt-2").isEmpty());
        assertEquals(0, store.purge(Instant.now().plus(1, ChronoUnit.DAYS)));
    }

    @Test
    void 开启但没密钥时启动失败() {
        assertThrows(IllegalStateException.class, () -> store(true, null));
        assertThrows(IllegalStateException.class, () -> store(true, "not-base64!!"));
    }

    @Test
    void 密钥轮换后旧key_id的原文仍可读() {
        String newKey = Base64.getEncoder().encodeToString("abcdefghijklmnopqrstuvwxyz123456".getBytes());
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:rotate-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");

        LakeProperties old = new LakeProperties();
        old.getAudit().setSqlCaptureEnabled(true);
        old.getAudit().setKeyId("v1");
        old.getAudit().setKey(KEY);
        AuditTextStore before = new AuditTextStore(old, new JdbcTemplate(ds), new SimpleMeterRegistry());
        before.init();
        before.save("evt-rotate", "operator", null, "select 1");

        LakeProperties rotated = new LakeProperties();
        rotated.getAudit().setSqlCaptureEnabled(true);
        rotated.getAudit().setKeyId("v2");
        rotated.getAudit().setKey(newKey);
        rotated.getAudit().getKeys().put("v1", KEY);
        AuditTextStore after = new AuditTextStore(rotated, new JdbcTemplate(ds), new SimpleMeterRegistry());
        after.init();
        AuditTextStore.TextRecord record = after.find("evt-rotate").orElseThrow();
        assertEquals("v1", record.keyId());
        assertEquals("select 1", record.sql());

        LakeProperties noHistory = new LakeProperties();
        noHistory.getAudit().setSqlCaptureEnabled(true);
        noHistory.getAudit().setKeyId("v2");
        noHistory.getAudit().setKey(newKey);
        AuditTextStore broken = new AuditTextStore(noHistory, new JdbcTemplate(ds), new SimpleMeterRegistry());
        broken.init();
        assertThrows(IllegalStateException.class, () -> broken.find("evt-rotate"));
    }

    @Test
    void 法律保全阻止过期清理() {
        AuditTextStore store = store(true, KEY);
        store.save("evt-3", "operator", null, "select 1");
        Instant future = Instant.now().plus(2, ChronoUnit.DAYS);

        assertTrue(store.setLegalHold("evt-3", true));
        assertEquals(0, store.purge(future));
        assertTrue(store.find("evt-3").orElseThrow().legalHold());

        assertTrue(store.setLegalHold("evt-3", false));
        assertEquals(1, store.purge(future));
        assertTrue(store.find("evt-3").isEmpty());
    }
}
