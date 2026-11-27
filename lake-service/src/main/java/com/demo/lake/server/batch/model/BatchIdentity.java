package com.demo.lake.server.batch.model;

import org.springframework.stereotype.Component;

import java.util.UUID;

/** 本服务实例的身份：心跳归属与调度认领共用，避免多实例互相续命/抢单 */
@Component
public final class BatchIdentity {

    private final String id = "svc-" + UUID.randomUUID().toString().substring(0, 8);

    public String id() {
        return id;
    }
}
