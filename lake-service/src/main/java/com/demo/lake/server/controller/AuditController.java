package com.demo.lake.server.controller;

import com.demo.lake.server.observability.AuditBuffer;
import com.demo.lake.server.observability.AuditEvent;
import com.demo.lake.server.observability.AuditService;
import com.demo.lake.server.observability.AuditTextStore;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "审计", description = "最近审计事件与查询原文保险箱（原文读取需 AUDITOR 角色，且每次读取被再审计）")
public class AuditController {

    private final AuditBuffer buffer;
    private final AuditTextStore texts;
    private final AuditService audit;

    @Operation(summary = "最近审计事件", description = "环形缓冲里的近期事件，按时间倒序（最新在前）；只含指纹等元数据，不含 SQL 原文")
    @GetMapping
    public List<AuditEvent> recent(
            @Parameter(description = "最多返回多少条，默认 100") @RequestParam(defaultValue = "100") int limit) {
        return buffer.recent(limit);
    }

    @Operation(summary = "按事件取回查询原文", description = "合规调查用；redacted 字段标明是否已脱敏。需要 AUDITOR 角色，每次读取都会再记一条审计")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "原文记录"),
            @ApiResponse(responseCode = "404", description = "没有这条记录，或保险箱未开启")
    })
    @GetMapping("/sql/{eventId}")
    public ResponseEntity<Map<String, Object>> text(
            @Parameter(description = "审计事件 ID") @PathVariable String eventId,
            @Parameter(description = "调阅理由，记为审计事件的属性") @RequestParam(defaultValue = "compliance-review") String reason,
            @Parameter(hidden = true) Principal principal) {
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

    @Operation(summary = "法律保全开关", description = "置位后原文不按 TTL 清理。需要 AUDITOR 角色")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "操作成功"),
            @ApiResponse(responseCode = "404", description = "没有这条查询原文")
    })
    @PostMapping("/sql/{eventId}/hold")
    public Map<String, Object> hold(
            @Parameter(description = "审计事件 ID") @PathVariable String eventId,
            @Parameter(description = "true=置位保全，false=解除") @RequestParam(defaultValue = "true") boolean hold,
            @Parameter(hidden = true) Principal principal) {
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
