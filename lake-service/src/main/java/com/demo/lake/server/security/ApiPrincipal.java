package com.demo.lake.server.security;

import java.security.Principal;
import java.util.List;

/** 认证成功后挂到 exchange 上的主体：查询配额按 name 分桶，角色由过滤器校验 */
public record ApiPrincipal(String name, List<String> roles) implements Principal {
    @Override
    public String getName() {
        return name;
    }
}
