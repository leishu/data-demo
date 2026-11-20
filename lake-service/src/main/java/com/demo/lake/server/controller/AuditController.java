package com.demo.lake.server.controller;

import com.demo.lake.server.observability.AuditBuffer;
import com.demo.lake.server.observability.AuditEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** 最近审计事件的操作视图（完整留存看 logs/audit.log） */
@RestController
@RequestMapping("/api/audit")
@RequiredArgsConstructor
public class AuditController {

    private final AuditBuffer buffer;

    @GetMapping
    public List<AuditEvent> recent(@RequestParam(defaultValue = "100") int limit) {
        return buffer.recent(limit);
    }
}
