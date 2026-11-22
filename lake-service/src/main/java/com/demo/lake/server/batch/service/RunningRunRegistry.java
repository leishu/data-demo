package com.demo.lake.server.batch.service;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本实例「确实还有执行器在盯着」的 run 集合。
 *
 * <p>心跳由它驱动：只有这里登记过的 run 才续 heartbeat_ms。以前按 owner 批量续命，
 * 一次终态写库失败就会让僵尸 RUNNING 永远收到心跳，对账也救不回来；现在心跳和
 * 执行器的存活状态绑在一起，终态回调（或写库失败）把 id 摘掉后，对账就能按时收尸。</p>
 */
@Component
public final class RunningRunRegistry {

    private final Set<String> ids = ConcurrentHashMap.newKeySet();

    public void add(String runId) {
        ids.add(runId);
    }

    public void remove(String runId) {
        ids.remove(runId);
    }

    public Set<String> snapshot() {
        return Set.copyOf(ids);
    }
}
