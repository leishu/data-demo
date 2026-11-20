package com.demo.lake.server.observability;

import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** 最近若干条审计事件的操作视图；真正的审计留存以 logs/audit.log 为准 */
@Component
public class AuditBuffer {

    private static final int MAX = 200;
    private final Deque<AuditEvent> events = new ArrayDeque<>();

    public synchronized void add(AuditEvent event) {
        events.addLast(event);
        while (events.size() > MAX) {
            events.pollFirst();
        }
    }

    public synchronized List<AuditEvent> recent(int limit) {
        int n = Math.max(1, Math.min(limit, MAX));
        List<AuditEvent> all = new ArrayList<>(events);
        return List.copyOf(all.subList(Math.max(0, all.size() - n), all.size()));
    }
}
