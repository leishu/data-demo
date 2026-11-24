package com.demo.lake.server.observability;

import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
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

    /** 取最近 n 条，按时间倒序（最新在前）返回 */
    public synchronized List<AuditEvent> recent(int limit) {
        int n = Math.clamp(limit, 1, MAX);
        List<AuditEvent> result = new ArrayList<>(n);
        for (Iterator<AuditEvent> it = events.descendingIterator(); it.hasNext() && result.size() < n; ) {
            result.add(it.next());
        }
        return List.copyOf(result);
    }
}
