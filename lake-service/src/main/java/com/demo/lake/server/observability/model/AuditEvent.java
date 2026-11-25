package com.demo.lake.server.observability.model;

import java.time.Instant;
import java.util.Map;

/** 一条审计事件：time / event / actor + 结构化字段（SQL 只留指纹，不记原文） */
public record AuditEvent(Instant at, String event, String actor, Map<String, String> fields) {
}
