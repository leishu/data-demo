package com.demo.lake.server.controller;

import com.demo.lake.server.observability.AuditBuffer;
import com.demo.lake.server.observability.AuditEvent;
import com.demo.lake.server.observability.AuditService;
import com.demo.lake.server.observability.AuditTextStore;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.CacheControl;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.security.Principal;
import java.util.List;
import java.util.Map;

/** 最近审计事件的操作视图；原文保险箱单独授权（AUDITOR），且每次读取都被再审计 */
@RestController
@RequestMapping("/api/audit")
@RequiredArgsConstructor
public class AuditController {

    private final AuditBuffer buffer;
    private final AuditTextStore texts;
    private final AuditService audit;

    @GetMapping
    public List<AuditEvent> recent(@RequestParam(defaultValue = "100") int limit) {
        return buffer.recent(limit);
    }

    /** 合规调查用：按 event_id 取回原文（脱敏情况由 redacted 字段标明） */
    @GetMapping("/sql/{eventId}")
    public ResponseEntity<Map<String, Object>> text(@PathVariable String eventId,
                                                     @RequestParam(defaultValue = "compliance-review") String reason,
                                                     Principal principal) {
        AuditTextStore.TextRecord record = texts.find(eventId).orElseThrow(() ->
                new ResponseStatusException(HttpStatus.NOT_FOUND, "没有这条查询原文，或保险箱未开启"));
        audit.record("audit.text_access", actor(principal),
                Map.of("event_id", eventId, "reason", reason));

        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(Map.of(
                        "event_id", record.eventId(),
                        "actor", record.actor(),
                        "client_ip", record.clientIp() == null ? "" : record.clientIp(),
                        "at_ms", record.atMs(),
                        "expires_at", record.expiresAt(),
                        "legal_hold", record.legalHold(),
                        "key_id", record.keyId(),
                        "redacted", record.redacted(),
                        "sql", record.sql()));
    }

    /** 法律保全：置位后不按 TTL 清理 */
    @PostMapping("/sql/{eventId}/hold")
    public Map<String, Object> hold(@PathVariable String eventId,
                                    @RequestParam(defaultValue = "true") boolean hold,
                                    Principal principal) {
        boolean ok = texts.setLegalHold(eventId, hold);
        audit.record("audit.text_hold", actor(principal),
                Map.of("event_id", eventId, "hold", String.valueOf(hold)));
        if (!ok) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "没有这条查询原文");
        }
        return Map.of("event_id", eventId, "legal_hold", hold);
    }

    private static String actor(Principal principal) {
        return principal == null || principal.getName() == null ? "anonymous" : principal.getName();
    }
}
