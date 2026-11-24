package com.demo.lake.server.observability;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuditServiceTest {

    @Test
    void 审计只留指纹不泄露SQL原文() {
        AuditBuffer buffer = new AuditBuffer();
        AuditService audit = new AuditService(buffer);
        String sql = "select * from gold_daily_revenue where dt = '2026-11-20'";

        audit.record("query.execute", "operator", Map.of(
                "sql_fp", AuditService.fingerprint(sql), "rows", 42, "outcome", "success"));

        List<AuditEvent> events = buffer.recent(10);
        assertEquals(1, events.size());
        AuditEvent event = events.get(0);
        assertEquals("query.execute", event.event());
        assertEquals("operator", event.actor());
        assertEquals("42", event.fields().get("rows"));
        assertEquals(16, event.fields().get("sql_fp").length());
        assertFalse(event.fields().toString().contains("gold_daily_revenue"));
        assertEquals(AuditService.fingerprint(sql), AuditService.fingerprint(sql));
        assertTrue(AuditService.fingerprint(null).equals("-"));
    }

    @Test
    void 环形缓冲只保留最近N条() {
        AuditBuffer buffer = new AuditBuffer();
        AuditService audit = new AuditService(buffer);
        for (int i = 0; i < 205; i++) {
            audit.record("event." + i, "tester", Map.of("i", i));
        }
        List<AuditEvent> events = buffer.recent(500);
        assertEquals(200, events.size());
        // 时间倒序：最新的在前
        assertEquals("event.204", events.get(0).event());
        assertEquals("event.5", events.get(199).event());
    }
}
